#include <iostream>
#include "duckdb/parallel/pipeline.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/main/database.hpp"

#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/parallel/pipeline_event.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/execution/operator/polr/physical_multiplexer.hpp"

namespace duckdb {

class PipelineTask : public ExecutorTask {
	static constexpr const idx_t PARTIAL_CHUNK_COUNT = 50;

public:
	explicit PipelineTask(Pipeline &pipeline_p, shared_ptr<Event> event_p)
	    : ExecutorTask(pipeline_p.executor), pipeline(pipeline_p), event(move(event_p)) {
	}

	Pipeline &pipeline;
	shared_ptr<Event> event;
	unique_ptr<PipelineExecutor> pipeline_executor;

public:
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		if (!pipeline_executor) {
			pipeline_executor = make_unique<PipelineExecutor>(pipeline.GetClientContext(), pipeline);
		}
		if (mode == TaskExecutionMode::PROCESS_PARTIAL) {
			bool finished = pipeline_executor->Execute(PARTIAL_CHUNK_COUNT);
			if (!finished) {
				return TaskExecutionResult::TASK_NOT_FINISHED;
			}
		} else {
			pipeline_executor->Execute();
		}
		event->FinishTask();
		pipeline_executor.reset();
		return TaskExecutionResult::TASK_FINISHED;
	}
};

Pipeline::Pipeline(Executor &executor_p)
    : executor(executor_p), ready(false), initialized(false), source(nullptr), sink(nullptr) {
}

ClientContext &Pipeline::GetClientContext() {
	return executor.context;
}

bool Pipeline::GetProgress(double &current_percentage, idx_t &source_cardinality) {
	D_ASSERT(source);
	source_cardinality = source->estimated_cardinality;
	if (!initialized) {
		current_percentage = 0;
		return true;
	}
	auto &client = executor.context;
	current_percentage = source->GetProgress(client, *source_state);
	return current_percentage >= 0;
}

void Pipeline::ScheduleSequentialTask(shared_ptr<Event> &event) {
	vector<unique_ptr<Task>> tasks;
	tasks.push_back(make_unique<PipelineTask>(*this, event));
	event->SetTasks(move(tasks));
}

bool Pipeline::ScheduleParallel(shared_ptr<Event> &event) {
	// check if the sink, source and all intermediate operators support parallelism
	if (!sink->ParallelSink()) {
		return false;
	}
	if (!source->ParallelSource()) {
		return false;
	}
	for (auto &op : operators) {
		if (!op->ParallelOperator()) {
			return false;
		}
	}
	if (sink->RequiresBatchIndex()) {
		if (!source->SupportsBatchIndex()) {
			throw InternalException(
			    "Attempting to schedule a pipeline where the sink requires batch index but source does not support it");
		}
	}
	idx_t max_threads = source_state->MaxThreads();
	return LaunchScanTasks(event, max_threads);
}

bool Pipeline::IsOrderDependent() const {
	auto &config = DBConfig::GetConfig(executor.context);
	if (!config.options.preserve_insertion_order) {
		return false;
	}
	if (sink && sink->IsOrderDependent()) {
		return true;
	}
	if (source->IsOrderDependent()) {
		return true;
	}
	for (auto &op : operators) {
		if (op->IsOrderDependent()) {
			return true;
		}
	}
	return false;
}

void Pipeline::Schedule(shared_ptr<Event> &event) {
	D_ASSERT(ready);
	D_ASSERT(sink);
	Reset();
	if (!ScheduleParallel(event)) {
		// could not parallelize this pipeline: push a sequential task instead
		ScheduleSequentialTask(event);
	}
}

bool Pipeline::LaunchScanTasks(shared_ptr<Event> &event, idx_t max_threads) {
	// split the scan up into parts and schedule the parts
	auto &scheduler = TaskScheduler::GetScheduler(executor.context);
	idx_t active_threads = scheduler.NumberOfThreads();
	if (max_threads > active_threads) {
		max_threads = active_threads;
	}
	if (max_threads <= 1) {
		// too small to parallelize
		return false;
	}

	// launch a task for every thread
	vector<unique_ptr<Task>> tasks;
	for (idx_t i = 0; i < max_threads; i++) {
		tasks.push_back(make_unique<PipelineTask>(*this, event));
	}
	event->SetTasks(move(tasks));
	return true;
}

void Pipeline::Reset() {
	if (sink && !sink->sink_state) {
		sink->sink_state = sink->GetGlobalSinkState(GetClientContext());
	}

	for (auto &op : operators) {
		if (op && !op->op_state) {
			op->op_state = op->GetGlobalOperatorState(GetClientContext());
		}
	}
	ResetSource();
	initialized = true;
}

void Pipeline::ResetSource() {
	source_state = source->GetGlobalSourceState(GetClientContext());
}

void Pipeline::Ready() {
	if (ready) {
		return;
	}
	ready = true;
	std::reverse(operators.begin(), operators.end());
	Reset(); // TODO: Why do we have to reset here?

	if (executor.context.config.enable_polr && executor.context.polr_paths) {
		BuildPOLRPaths();
	}
}

void Pipeline::Finalize(Event &event) {
	if (executor.HasError()) {
		return;
	}
	D_ASSERT(ready);
	try {
		auto sink_state = sink->Finalize(*this, event, executor.context, *sink->sink_state);
		sink->sink_state->state = sink_state;
	} catch (Exception &ex) { // LCOV_EXCL_START
		executor.PushError(PreservedError(ex));
	} catch (std::exception &ex) {
		executor.PushError(PreservedError(ex));
	} catch (...) {
		executor.PushError(PreservedError("Unknown exception in Finalize!"));
	} // LCOV_EXCL_STOP
}

void Pipeline::AddDependency(shared_ptr<Pipeline> &pipeline) {
	D_ASSERT(pipeline);
	dependencies.push_back(weak_ptr<Pipeline>(pipeline));
	pipeline->parents.push_back(weak_ptr<Pipeline>(shared_from_this()));
}

string Pipeline::ToString() const {
	TreeRenderer renderer;
	return renderer.ToString(*this);
}

void Pipeline::Print() const {
	Printer::Print(ToString());
}

vector<PhysicalOperator *> Pipeline::GetOperators() const {
	vector<PhysicalOperator *> result;
	D_ASSERT(source);
	result.push_back(source);
	result.insert(result.end(), operators.begin(), operators.end());
	if (sink) {
		result.push_back(sink);
	}
	return result;
}

void Pipeline::BuildPOLRPaths() {
	if (operators.empty()) {
		return;
	}

	vector<idx_t> hash_join_idxs;
	for (idx_t i = 0; i < operators.size(); i++) {
		if (operators[i]->type == PhysicalOperatorType::HASH_JOIN) {
			// We only want joins that directly follow each other
			if (hash_join_idxs.empty() || hash_join_idxs.back() == i - 1) {
				hash_join_idxs.push_back(i);
			} else {
				break;
			}
		}
	}

	if (hash_join_idxs.size() >= 2) {
		auto &initial_join_path = executor.context.polr_paths->at(0);
		if (hash_join_idxs.size() != initial_join_path.size() - 1) {
			return;
		}

		D_ASSERT(hash_join_idxs.size() == initial_join_path.size() - 1);

		map<idx_t, idx_t> join_order_mapping;

		for (idx_t i = 1; i < initial_join_path.size(); i++) {
			join_order_mapping[initial_join_path[i]] = i - 1;
		}

		join_paths.reserve(executor.context.polr_paths->size());

		// Fill join paths
		for (auto &path : *executor.context.polr_paths) {
			vector<idx_t> translated_join_order;
			translated_join_order.reserve(path.size());

			for (idx_t i = 1; i < path.size(); i++) {
				idx_t relation_idx = path[i];
				idx_t translated_join_idx = join_order_mapping[relation_idx];
				translated_join_order.push_back(translated_join_idx);
			}

			join_paths.push_back(translated_join_order);
		}

		vector<idx_t> num_columns_per_join;
		num_columns_per_join.reserve(hash_join_idxs.size());

		for (auto hash_join_idx : hash_join_idxs) {
			joins.push_back(static_cast<PhysicalHashJoin *>(operators[hash_join_idx]));
			num_columns_per_join.push_back(joins.back()->types.size());
		}

		// Remove joins from operator vector
		operators.erase(operators.begin() + hash_join_idxs.front(),
		                operators.begin() + hash_join_idxs.front() + hash_join_idxs.size());

		auto prev_types = joins.front()->children[0]->GetTypes();
		multiplexer = make_unique<PhysicalMultiplexer>(prev_types, joins.front()->children[0]->estimated_cardinality,
		                                               join_paths.size());
		multiplexer->op_state = multiplexer->GetGlobalOperatorState(executor.context);
		multiplexer_idx = hash_join_idxs.front();
		operators.insert(operators.begin() + multiplexer_idx, &*multiplexer);

		adaptive_union =
		    make_unique<PhysicalAdaptiveUnion>(joins.back()->types, multiplexer->types.size(),
		                                       move(num_columns_per_join), joins.back()->estimated_cardinality);
		adaptive_union->op_state = adaptive_union->GetGlobalOperatorState(executor.context);

		// Depending on the join order, the join conditions may have to use different columns idxs for probing.
		// Here we build a map that the pipeline executor can use to build JoinOperatorStates containing
		// expressions for the different probe column idxs.
		left_expression_bindings.reserve(join_paths.size());

		const idx_t seed_table_column_count = prev_types.size();
		vector<idx_t> column_offsets;
		column_offsets.reserve(joins.size() + 1);
		column_offsets.push_back(seed_table_column_count);

		// join_idx, join_condition_idx, probe_side_join_idx, relative_column_idx
		map<idx_t, map<idx_t, pair<idx_t, idx_t>>> relative_column_binding_map;

		for (idx_t i = 0; i < joins.size(); i++) {
			auto &join = joins[i];
			idx_t num_columns_from_right = join->right_projection_map.empty() ? join->children[1]->types.size()
			                                                                  : join->right_projection_map.size();
			column_offsets.push_back(column_offsets.back() + num_columns_from_right);

			for (idx_t j = 0; j < join->conditions.size(); j++) {
				auto &condition = join->conditions[j];
				auto &left_bound_expression = dynamic_cast<BoundReferenceExpression &>(*condition.left);

				if (left_bound_expression.index >= seed_table_column_count) {
					for (idx_t k = 1; k < column_offsets.size(); k++) {
						if (column_offsets[k] > left_bound_expression.index) {
							idx_t join_idx = k - 1;
							idx_t relative_column_idx = left_bound_expression.index - column_offsets[join_idx];
							relative_column_binding_map[i][j] = make_pair(join_idx, relative_column_idx);
							break;
						}
					}
				}
			}
		}

		for (auto &join_path : join_paths) {
			vector<map<idx_t, idx_t>> expression_bindings(join_path.size());
			vector<idx_t> current_join_path_column_offsets;

			current_join_path_column_offsets.reserve(join_path.size() + 1);
			current_join_path_column_offsets.push_back(seed_table_column_count);

			for (idx_t j = 0; j < join_path.size(); j++) {
				auto &join_idx = join_path[j];
				auto column_bindings = relative_column_binding_map.find(join_idx);
				if (column_bindings != relative_column_binding_map.cend()) {
					auto &binding_map = column_bindings->second;

					for (auto &binding : binding_map) {
						auto &[probe_join_idx, relative_column_idx] = binding.second;

						for (idx_t i = 0; i < current_join_path_column_offsets.size(); i++) {
							if (join_path[i] == probe_join_idx) {
								idx_t probe_column_idx = current_join_path_column_offsets[i] + relative_column_idx;
								expression_bindings[j][binding.first] = probe_column_idx;
							}
						}
					}
				}

				current_join_path_column_offsets.push_back(current_join_path_column_offsets.back() +
				                                           joins[join_idx]->right_projection_map.size());
			}

			left_expression_bindings.push_back(expression_bindings);
		}
	}
}

//===--------------------------------------------------------------------===//
// Pipeline Build State
//===--------------------------------------------------------------------===//
void PipelineBuildState::SetPipelineSource(Pipeline &pipeline, PhysicalOperator *op) {
	pipeline.source = op;
}

void PipelineBuildState::SetPipelineSink(Pipeline &pipeline, PhysicalOperator *op) {
	pipeline.sink = op;
	// set the base batch index of this pipeline based on how many other pipelines have this node as their sink
	pipeline.base_batch_index = BATCH_INCREMENT * sink_pipeline_count[op];
	// increment the number of nodes that have this pipeline as their sink
	sink_pipeline_count[op]++;
}

void PipelineBuildState::AddPipelineOperator(Pipeline &pipeline, PhysicalOperator *op) {
	pipeline.operators.push_back(op);
}

void PipelineBuildState::AddPipeline(Executor &executor, shared_ptr<Pipeline> pipeline) {
	executor.pipelines.push_back(move(pipeline));
}

PhysicalOperator *PipelineBuildState::GetPipelineSource(Pipeline &pipeline) {
	return pipeline.source;
}

PhysicalOperator *PipelineBuildState::GetPipelineSink(Pipeline &pipeline) {
	return pipeline.sink;
}

void PipelineBuildState::SetPipelineOperators(Pipeline &pipeline, vector<PhysicalOperator *> operators) {
	pipeline.operators = move(operators);
}

void PipelineBuildState::AddChildPipeline(Executor &executor, Pipeline &pipeline) {
	executor.AddChildPipeline(&pipeline);
}

unordered_map<Pipeline *, vector<shared_ptr<Pipeline>>> &PipelineBuildState::GetUnionPipelines(Executor &executor) {
	return executor.union_pipelines;
}
unordered_map<Pipeline *, vector<shared_ptr<Pipeline>>> &PipelineBuildState::GetChildPipelines(Executor &executor) {
	return executor.child_pipelines;
}
vector<PhysicalOperator *> PipelineBuildState::GetPipelineOperators(Pipeline &pipeline) {
	return pipeline.operators;
}

} // namespace duckdb
