#include "duckdb/parallel/pipeline.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/pipeline_event.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/parallel/polar_enumeration_algo.hpp"
#include "duckdb/parallel/polar_pipeline_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/thread_context.hpp"

#include <fstream>
#include <functional>
#include <iostream>
#include <unistd.h>

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
			if (pipeline.polar_config) {
				pipeline_executor = make_unique<POLARPipelineExecutor>(pipeline.GetClientContext(), pipeline);
			} else {
				pipeline_executor = make_unique<PipelineExecutor>(pipeline.GetClientContext(), pipeline);
			}
		}
		if (mode == TaskExecutionMode::PROCESS_PARTIAL && !pipeline.is_backpressure_pipeline &&
		    !pipeline.polar_config) {
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

vector<PhysicalOperator *> &Pipeline::GetOperators() {
	return polar_config ? polar_config->operators : operators;
}

bool Pipeline::GetProgress(double &current_percentage, idx_t &source_cardinality) {
	D_ASSERT(source);
	source_cardinality = source->estimated_cardinality;
	if (!initialized) {
		current_percentage = 0;
		return true;
	}
	auto &client = executor.context;
	current_percentage =
	    source->GetProgress(client, is_backpressure_pipeline ? *polar_config->source_state : *source_state);
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
	begin = std::chrono::system_clock::now();
	if (!ScheduleParallel(event)) {
		// could not parallelize this pipeline: push a sequential task instead
		ScheduleSequentialTask(event);
	}
}

bool Pipeline::LaunchScanTasks(shared_ptr<Event> &event, idx_t max_threads) {
	auto &scheduler = TaskScheduler::GetScheduler(executor.context);
	auto routing = DBConfig::GetConfig(executor.context).options.multiplexer_routing;
	if (routing == MultiplexerRouting::BACKPRESSURE && polar_config) {
		// scheduler.SetThreads(polar_config->backpressure_pipelines->size());
		vector<unique_ptr<Task>> tasks;
		for (idx_t i = 0; i < polar_config->backpressure_pipelines->size(); i++) {
			tasks.push_back(make_unique<PipelineTask>(*polar_config->backpressure_pipelines->at(i), event));
		}
		event->SetTasks(move(tasks));
		return true;
	}
	// split the scan up into parts and schedule the parts
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
	Reset();

	if (executor.context.config.enable_polr || executor.context.config.measure_polr_pipeline) {
		polar_config = make_shared<POLARConfig>(this, JoinEnumerationAlgo::CreateEnumerationAlgo(executor.context));
		bool join_orders_generated = polar_config->GenerateJoinOrders();
		if (!join_orders_generated || !executor.context.config.enable_polr) {
			polar_config = nullptr;
		} else if (polar_config->backpressure_pipelines) {
			for (auto &pipeline : *polar_config->backpressure_pipelines) {
				pipeline->polar_config = polar_config;
			}
		}

		measure_pipeline_duration = join_orders_generated && executor.context.config.measure_polr_pipeline;
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

		if (measure_pipeline_duration) {
			auto end = std::chrono::system_clock::now();

			std::string filename = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
			std::ofstream file;

			auto source_str = source->ParamsToString();
			auto source_hash = std::hash<string> {}(source_str);
			char tmp[256];
			getcwd(tmp, 256);
			file.open(std::string(tmp) + "/tmp/" + filename + "-" + to_string(source_hash) + ".csv");

			double duration_ms = std::chrono::duration<double, std::milli>(end - begin).count();
			file << duration_ms;
			file.close();
		}
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
