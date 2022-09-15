#include <iostream>
#include "duckdb/parallel/pipeline.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/main/database.hpp"

#include "duckdb/execution/operator/aggregate/physical_simple_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_window.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
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

Pipeline::Pipeline(Executor &executor_p) : executor(executor_p), ready(false), source(nullptr), sink(nullptr) {
}

ClientContext &Pipeline::GetClientContext() {
	return executor.context;
}

// LCOV_EXCL_START
bool Pipeline::GetProgressInternal(ClientContext &context, PhysicalOperator *op, double &current_percentage) {
	current_percentage = -1;
	switch (op->type) {
	case PhysicalOperatorType::TABLE_SCAN: {
		auto &get = (PhysicalTableScan &)*op;
		if (get.function.table_scan_progress) {
			current_percentage = get.function.table_scan_progress(context, get.bind_data.get());
			return true;
		}
		// If the table_scan_progress is not implemented it means we don't support this function yet in the progress
		// bar
		return false;
	}
		// If it is not a table scan we go down on all children until we reach the leaf operators
	default: {
		vector<idx_t> progress;
		vector<idx_t> cardinality;
		double total_cardinality = 0;
		current_percentage = 0;
		for (auto &op_child : op->children) {
			double child_percentage = 0;
			if (!GetProgressInternal(context, op_child.get(), child_percentage)) {
				return false;
			}
			if (!Value::DoubleIsFinite(child_percentage)) {
				return false;
			}
			progress.push_back(child_percentage);
			cardinality.push_back(op_child->estimated_cardinality);
			total_cardinality += op_child->estimated_cardinality;
		}
		for (size_t i = 0; i < progress.size(); i++) {
			current_percentage += progress[i] * cardinality[i] / total_cardinality;
		}
		return true;
	}
	}
}
// LCOV_EXCL_STOP

bool Pipeline::GetProgress(double &current_percentage) {
	auto &client = executor.context;
	return GetProgressInternal(client, source, current_percentage);
}

void Pipeline::ScheduleSequentialTask(shared_ptr<Event> &event) {
	vector<unique_ptr<Task>> tasks;
	tasks.push_back(make_unique<PipelineTask>(*this, event));
	event->SetTasks(move(tasks));
}

bool Pipeline::ScheduleParallel(shared_ptr<Event> &event) {
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
	idx_t max_threads = source_state->MaxThreads();
	return LaunchScanTasks(event, max_threads);
}

void Pipeline::Schedule(shared_ptr<Event> &event) {
	D_ASSERT(ready);
	D_ASSERT(sink);
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
	Reset();

	if (executor.context.config.enable_polr) {
		BuildPOLRPaths();
	}
}

void Pipeline::Finalize(Event &event) {
	D_ASSERT(ready);
	try {
		auto sink_state = sink->Finalize(*this, event, executor.context, *sink->sink_state);
		sink->sink_state->state = sink_state;
	} catch (Exception &ex) { // LCOV_EXCL_START
		executor.PushError(ex.type, ex.what());
	} catch (std::exception &ex) {
		executor.PushError(ExceptionType::UNKNOWN_TYPE, ex.what());
	} catch (...) {
		executor.PushError(ExceptionType::UNKNOWN_TYPE, "Unknown exception in Finalize!");
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
			idx_t num_columns_from_right = join->right_projection_map.empty() ? join->children[1]->types.size() : join->right_projection_map.size();
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

} // namespace duckdb
