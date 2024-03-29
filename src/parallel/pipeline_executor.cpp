#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/limits.hpp"

#include <chrono>
#include <fstream>

namespace duckdb {

PipelineExecutor::PipelineExecutor(ClientContext &context_p, Pipeline &pipeline_p)
    : pipeline(pipeline_p), thread(context_p), context(context_p, thread) {
	D_ASSERT(pipeline.source_state || (pipeline.polar_config && pipeline.polar_config->source_state));
	auto &global_source_state =
	    pipeline.is_backpressure_pipeline ? *pipeline.polar_config->source_state : *pipeline.source_state;
	local_source_state = pipeline.source->GetLocalSourceState(context, global_source_state);
	if (pipeline.sink) {
		local_sink_state = pipeline.sink->GetLocalSinkState(context);
		requires_batch_index = pipeline.sink->RequiresBatchIndex() && pipeline.source->SupportsBatchIndex();
	}
	bool can_cache_in_pipeline = pipeline.sink && !pipeline.IsOrderDependent() && !requires_batch_index;

	auto &operators = pipeline.GetOperators();

	intermediate_chunks.reserve(operators.size());
	intermediate_states.reserve(operators.size());
	cached_chunks.resize(operators.size());

	for (idx_t i = 0; i < operators.size(); i++) {
		auto prev_operator = i == 0 ? pipeline.source : operators[i - 1];
		auto current_operator = operators[i];
		auto chunk = make_unique<DataChunk>();

		if (prev_operator->type == PhysicalOperatorType::MULTIPLEXER) {
			chunk->Initialize(Allocator::Get(context.client), pipeline.polar_config->adaptive_union->GetTypes());
		} else {
			chunk->Initialize(Allocator::Get(context.client), prev_operator->GetTypes());
		}
		intermediate_states.push_back(current_operator->GetOperatorState(context));

		intermediate_chunks.push_back(move(chunk));

		if (can_cache_in_pipeline && current_operator->RequiresCache()) {
			auto &cache_types = current_operator->GetTypes();
			bool can_cache = true;
			for (auto &type : cache_types) {
				if (!CanCacheType(type)) {
					can_cache = false;
					break;
				}
			}
			if (!can_cache) {
				continue;
			}
			cached_chunks[i] = make_unique<DataChunk>();
			cached_chunks[i]->Initialize(Allocator::Get(context.client), current_operator->GetTypes());
		}
		if (current_operator->IsSink() && current_operator->sink_state->state == SinkFinalizeType::NO_OUTPUT_POSSIBLE) {
			// one of the operators has already figured out no output is possible
			// we can skip executing the pipeline
			FinishProcessing();
		}
	}
	// Initialize final chunk
	PhysicalOperator *last_op = operators.empty() ? pipeline.source : operators.back();
	if (last_op->type == PhysicalOperatorType::MULTIPLEXER) {
		final_chunk.Initialize(Allocator::Get(context.client), pipeline.polar_config->adaptive_union->GetTypes());
	} else {
		final_chunk.Initialize(Allocator::Get(context.client), last_op->GetTypes());
	}

	if (pipeline.is_lip_pipeline) {
		auto &first_chunk = operators.empty() ? final_chunk : *intermediate_chunks[0];
		for (idx_t i = 0; i < operators.size(); i++) {
			auto *op = operators[i];
			if (op->type == PhysicalOperatorType::HASH_JOIN && ((PhysicalHashJoin *)op)->build_bloom_filter) {

				lip_join_idxs.push_back(i);
				auto chunk = make_unique<DataChunk>();
				chunk->Initialize(Allocator::Get(context.client), first_chunk.GetTypes());
				lip_chunks.push_back(move(chunk));
				lip_statistics.emplace(i, make_pair(0, 0));
			}
		}
	}
}

bool PipelineExecutor::Execute(idx_t max_chunks) {
	D_ASSERT(pipeline.sink);
	auto now = std::chrono::system_clock::now();
	bool exhausted_source = false;
	auto &operators = pipeline.GetOperators();
	auto &source_chunk = operators.empty() ? final_chunk : *intermediate_chunks[0];
	for (idx_t i = 0; i < max_chunks; i++) {
		if (IsFinished()) {
			break;
		}
		source_chunk.Reset();
		FetchFromSource(source_chunk);
		if (source_chunk.size() == 0) {
			exhausted_source = true;
			break;
		}
		auto result = ExecutePushInternal(source_chunk);
		if (result == OperatorResultType::FINISHED) {
			D_ASSERT(IsFinished());
			break;
		}
	}
	if (!exhausted_source && !IsFinished()) {
		return false;
	}
	PushFinalize();

	return true;
}

void PipelineExecutor::Execute() {
	Execute(NumericLimits<idx_t>::Maximum());
}

OperatorResultType PipelineExecutor::ExecutePush(DataChunk &input) { // LCOV_EXCL_START
	return ExecutePushInternal(input);
} // LCOV_EXCL_STOP

void PipelineExecutor::FinishProcessing(int32_t operator_idx) {
	finished_processing_idx = operator_idx < 0 ? NumericLimits<int32_t>::Maximum() : operator_idx;
	in_process_operators = stack<idx_t>();
}

bool PipelineExecutor::IsFinished() {
	return finished_processing_idx >= 0;
}

OperatorResultType PipelineExecutor::ExecutePushInternal(DataChunk &input, idx_t initial_idx) {
	D_ASSERT(pipeline.sink);
	if (input.size() == 0) { // LCOV_EXCL_START
		return OperatorResultType::NEED_MORE_INPUT;
	} // LCOV_EXCL_STOP
	while (true) {
		OperatorResultType result;
		auto &operators = pipeline.GetOperators();
		if (!operators.empty()) {
			final_chunk.Reset();
			result = Execute(input, final_chunk, initial_idx);
			if (result == OperatorResultType::FINISHED) {
				return OperatorResultType::FINISHED;
			}
		} else {
			result = OperatorResultType::NEED_MORE_INPUT;
		}
		auto &sink_chunk = operators.empty() ? input : final_chunk;
		if (sink_chunk.size() > 0) {
			StartOperator(pipeline.sink);
			D_ASSERT(pipeline.sink);
			D_ASSERT(pipeline.sink->sink_state);
			auto sink_result = pipeline.sink->Sink(context, *pipeline.sink->sink_state, *local_sink_state, sink_chunk);
			EndOperator(pipeline.sink, nullptr);
			if (sink_result == SinkResultType::FINISHED) {
				FinishProcessing();
				return OperatorResultType::FINISHED;
			}
		}
		if (result == OperatorResultType::NEED_MORE_INPUT) {
			return OperatorResultType::NEED_MORE_INPUT;
		}
	}
}

void PipelineExecutor::PushFinalize() {
	if (finalized) {
		throw InternalException("Calling PushFinalize on a pipeline that has been finalized already");
	}
	finalized = true;
	// flush all caches
	// note that even if an operator has finished, we might still need to flush caches AFTER that operator
	// e.g. if we have SOURCE -> LIMIT -> CROSS_PRODUCT -> SINK, if the LIMIT reports no more rows will be passed on
	// we still need to flush caches from the CROSS_PRODUCT
	D_ASSERT(in_process_operators.empty());
	idx_t start_idx = IsFinished() ? idx_t(finished_processing_idx) : 0;
	for (idx_t i = start_idx; i < cached_chunks.size(); i++) {
		if (cached_chunks[i] && cached_chunks[i]->size() > 0) {
			ExecutePushInternal(*cached_chunks[i], i + 1);
			cached_chunks[i].reset();
		}
	}

	D_ASSERT(local_sink_state);
	// run the combine for the sink
	pipeline.sink->Combine(context, *pipeline.sink->sink_state, *local_sink_state);

	// flush all query profiler info
	auto &operators = pipeline.GetOperators();
	for (idx_t i = 0; i < intermediate_states.size(); i++) {
		intermediate_states[i]->Finalize(operators[i], context);
	}
	pipeline.executor.Flush(thread);
	local_sink_state.reset();
}

bool PipelineExecutor::CanCacheType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		return false;
	case LogicalTypeId::STRUCT: {
		auto &entries = StructType::GetChildTypes(type);
		for (auto &entry : entries) {
			if (!CanCacheType(entry.second)) {
				return false;
			}
		}
		return true;
	}
	default:
		return true;
	}
}

void PipelineExecutor::CacheChunk(DataChunk &current_chunk, idx_t operator_idx) {
#if STANDARD_VECTOR_SIZE >= 128
	if (!context.client.config.caching) {
		return;
	}

	DataChunk *chunk_cache = nullptr;

	if (cached_chunks[operator_idx]) {
		chunk_cache = &*cached_chunks[operator_idx];
	}

	if (chunk_cache) {
		if (current_chunk.size() < CACHE_THRESHOLD) {
			// we have filtered out a significant amount of tuples
			// add this chunk to the cache and continue
			chunk_cache->Append(current_chunk);
			if (chunk_cache->size() >= (STANDARD_VECTOR_SIZE - CACHE_THRESHOLD)) {
				// chunk cache full: return it
				current_chunk.Move(*chunk_cache);
				chunk_cache->Initialize(Allocator::Get(context.client), current_chunk.GetTypes());
			} else {
				// chunk cache not full: probe again
				current_chunk.Reset();
			}
		}
	}
#endif
}

void PipelineExecutor::ExecutePull(DataChunk &result) {
	if (IsFinished()) {
		return;
	}
	auto &executor = pipeline.executor;
	try {
		D_ASSERT(!pipeline.sink);
		auto &operators = pipeline.GetOperators();
		auto &source_chunk = operators.empty() ? result : *intermediate_chunks[0];
		while (result.size() == 0) {
			if (in_process_operators.empty()) {
				source_chunk.Reset();
				FetchFromSource(source_chunk);
				if (source_chunk.size() == 0) {
					break;
				}
			}
			if (operators.empty()) {
				auto state = Execute(source_chunk, result);
				if (state == OperatorResultType::FINISHED) {
					break;
				}
			}
		}
	} catch (const Exception &ex) { // LCOV_EXCL_START
		if (executor.HasError()) {
			executor.ThrowException();
		}
		throw;
	} catch (std::exception &ex) {
		if (executor.HasError()) {
			executor.ThrowException();
		}
		throw;
	} catch (...) {
		if (executor.HasError()) {
			executor.ThrowException();
		}
		throw;
	} // LCOV_EXCL_STOP
}

void PipelineExecutor::PullFinalize() {
	if (finalized) {
		throw InternalException("Calling PullFinalize on a pipeline that has been finalized already");
	}
	finalized = true;
	pipeline.executor.Flush(thread);
}

void PipelineExecutor::GoToSource(idx_t &current_idx, idx_t initial_idx) {
	// we go back to the first operator (the source)
	current_idx = initial_idx;
	if (!in_process_operators.empty()) {
		// ... UNLESS there is an in process operator
		// if there is an in-process operator, we start executing at the latest one
		// for example, if we have a join operator that has tuples left, we first need to emit those tuples
		current_idx = in_process_operators.top();
		in_process_operators.pop();
	}
	D_ASSERT(current_idx >= initial_idx);
}

OperatorResultType PipelineExecutor::Execute(DataChunk &input, DataChunk &result, idx_t initial_idx) {
	if (input.size() == 0) { // LCOV_EXCL_START
		return OperatorResultType::NEED_MORE_INPUT;
	} // LCOV_EXCL_STOP
	auto &operators = pipeline.GetOperators();
	// D_ASSERT(!operators.empty());

	idx_t current_idx;

	GoToSource(current_idx, initial_idx);
	if (current_idx == initial_idx) {
		current_idx++;
	}

	if (current_idx > operators.size()) {
		result.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	while (true) {
		if (context.client.interrupted) {
			throw InterruptException();
		}
		// now figure out where to put the chunk
		// if current_idx is the last possible index (>= operators.size()) we write to the result
		// otherwise we write to an intermediate chunk
		auto current_intermediate = current_idx;
		auto &current_chunk =
		    current_intermediate >= intermediate_chunks.size() ? result : *intermediate_chunks[current_intermediate];
		current_chunk.Reset();
		if (current_idx == initial_idx) {
			// we went back to the source: we need more input
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			auto *prev_chunk =
			    current_intermediate == initial_idx + 1 ? &input : &*intermediate_chunks[current_intermediate - 1];
			auto operator_idx = current_idx - 1;

			auto current_operator = operators[operator_idx];

			// if current_idx > source_idx, we pass the previous' operators output through the Execute of the
			// current operator
			StartOperator(current_operator);
			auto result = current_operator->Execute(context, *prev_chunk, current_chunk, *current_operator->op_state,
			                                        *intermediate_states[current_intermediate - 1]);
			EndOperator(current_operator, &current_chunk);
			if (result == OperatorResultType::HAVE_MORE_OUTPUT) {
				// more data remains in this operator
				// push in-process marker
				in_process_operators.push(current_idx);
			} else if (result == OperatorResultType::FINISHED) {
				D_ASSERT(current_chunk.size() == 0);
				FinishProcessing(current_idx);
				return OperatorResultType::FINISHED;
			}
			current_chunk.Verify();
			CacheChunk(current_chunk, operator_idx);
		}

		if (current_chunk.size() == 0) {
			// no output from this operator!
			if (current_idx == initial_idx) {
				// if we got no output from the scan, we are done
				break;
			} else {
				// if we got no output from an intermediate op
				// we go back and try to pull data from the source again
				GoToSource(current_idx, initial_idx);
				continue;
			}
		} else {
			// we got output! continue to the next operator
			current_idx++;
			if (current_idx > operators.size()) {
				// if we got output and are at the last operator, we are finished executing for this output chunk
				// return the data and push it into the chunk
				break;
			}
		}
	}

	return in_process_operators.empty() ? OperatorResultType::NEED_MORE_INPUT : OperatorResultType::HAVE_MORE_OUTPUT;
}

void PipelineExecutor::FetchFromSource(DataChunk &result) {
	D_ASSERT(in_process_operators.empty());
	StartOperator(pipeline.source);

	bool fetch_data = true;
	while (fetch_data) {
		fetch_data = pipeline.is_lip_pipeline;

		auto &source_result = pipeline.is_lip_pipeline ? *lip_chunks[0] : result;
		if (pipeline.is_lip_pipeline) {
			source_result.Reset();
		}

		auto &global_source_state =
		    pipeline.is_backpressure_pipeline ? *pipeline.polar_config->source_state : *pipeline.source_state;
		pipeline.source->GetData(context, source_result, global_source_state, *local_source_state);
		if (source_result.size() != 0 && requires_batch_index) {
			auto next_batch_index =
			    pipeline.source->GetBatchIndex(context, source_result, global_source_state, *local_source_state);
			next_batch_index += pipeline.base_batch_index;
			D_ASSERT(local_sink_state->batch_index <= next_batch_index ||
			         local_sink_state->batch_index == DConstants::INVALID_INDEX);
			local_sink_state->batch_index = next_batch_index;
		}

		if (source_result.size() == 0) {
			break;
		}

		if (pipeline.is_lip_pipeline) {
			lip_counter++;
			auto &operators = pipeline.GetOperators();
			for (idx_t i = 0; i < lip_join_idxs.size(); i++) {
				auto &next_chunk = i == lip_join_idxs.size() - 1 ? result : *lip_chunks[i + 1];
				next_chunk.Reset();
				idx_t join_idx = lip_join_idxs[i];
				auto *join = (PhysicalHashJoin *)operators[join_idx];
				join->ProbeBloomFilter(*lip_chunks[i], next_chunk, *intermediate_states[join_idx]);
				lip_statistics[join_idx].first += lip_chunks[i]->size();
				lip_statistics[join_idx].second += lip_chunks[i]->size() - next_chunk.size();
				if (next_chunk.size() == 0) {
					break;
				}
			}

			// Re-order
			if (lip_counter % LIP_THRESHOLD == 0) {
				std::sort(lip_join_idxs.begin(), lip_join_idxs.end(), [&](idx_t a, idx_t b) {
					double miss_rate_a =
					    lip_statistics[a].first == 0 ? 1 : (double)lip_statistics[a].second / lip_statistics[a].first;
					double miss_rate_b =
					    lip_statistics[b].first == 0 ? 1 : (double)lip_statistics[b].second / lip_statistics[b].first;
					return miss_rate_a > miss_rate_b;
				});
			}

			// Reset
			if (lip_counter >= LIP_THRESHOLD) {
				for (auto &statistic : lip_statistics) {
					statistic.second.first = 0;
					statistic.second.second = 0;
				}
				lip_counter = 0;
			}

			fetch_data = result.size() == 0;
		}
	}
	EndOperator(pipeline.source, &result);
}

void PipelineExecutor::InitializeChunk(DataChunk &chunk) {
	auto &operators = pipeline.GetOperators();
	PhysicalOperator *last_op = operators.empty() ? pipeline.source : operators.back();

	if (last_op->type == PhysicalOperatorType::MULTIPLEXER) {
		last_op = &*pipeline.polar_config->adaptive_union;
	}

	chunk.Initialize(Allocator::DefaultAllocator(), last_op->GetTypes());
}

void PipelineExecutor::StartOperator(PhysicalOperator *op) {
	if (context.client.interrupted) {
		throw InterruptException();
	}
	context.thread.profiler.StartOperator(op);
}

void PipelineExecutor::EndOperator(PhysicalOperator *op, DataChunk *chunk) {
	context.thread.profiler.EndOperator(chunk);

	if (chunk) {
		chunk->Verify();
	}
}

} // namespace duckdb
