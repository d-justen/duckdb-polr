#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/limits.hpp"

#include <chrono>
#include <fstream>

namespace duckdb {

PipelineExecutor::PipelineExecutor(ClientContext &context_p, Pipeline &pipeline_p)
    : pipeline(pipeline_p), thread(context_p), context(context_p, thread) {
	if (context.client.config.enable_polr) {
		if (!pipeline.joins.empty()) {
			join_intermediate_chunks.resize(pipeline.join_paths.size());
			cached_join_chunks.resize(pipeline.join_paths.front().size());

			for (idx_t i = 0; i < pipeline.join_paths.front().size(); i++) {
				cached_join_chunks[i] = make_unique<DataChunk>();
			}

			for (idx_t i = 0; i < pipeline.join_paths.size(); i++) {
				vector<LogicalType> types;
				types.reserve(pipeline.joins.back()->GetTypes().size());
				auto &multiplexer_types = pipeline.multiplexer->GetTypes();
				types.insert(types.cend(), multiplexer_types.cbegin(), multiplexer_types.cend());

				auto &current_join_path = pipeline.join_paths[i];
				join_intermediate_chunks[i].reserve(current_join_path.size());

				for (idx_t j = 0; j < current_join_path.size(); j++) {
					auto &build_types =
					    pipeline.joins[current_join_path[j]]
					        ->build_types; // TODO: check if this is the new types that come with the join
					types.insert(types.cend(), build_types.cbegin(), build_types.cend());

					auto chunk = make_unique<DataChunk>();
					chunk->Initialize(Allocator::Get(context.client), types);

					join_intermediate_chunks[i].push_back(move(chunk));
				}
			}

			join_intermediate_states.reserve(pipeline.join_paths.size());
			for (idx_t i = 0; i < pipeline.join_paths.size(); i++) {
				auto &join_path = pipeline.join_paths[i];
				vector<unique_ptr<OperatorState>> states;
				states.reserve(pipeline.joins.size());

				for (idx_t j = 0; j < pipeline.joins.size(); j++) {
					idx_t join_idx = join_path[j];
					auto *join = pipeline.joins[join_idx];
					auto &bindings = pipeline.left_expression_bindings[i][j];
					auto state = join->GetOperatorStateWithBindings(context, bindings);

					states.push_back(move(state));
				}
				join_intermediate_states.push_back(move(states));
			}

			mpx_output_chunk = make_unique<DataChunk>();
			mpx_output_chunk->Initialize(Allocator::Get(context.client), pipeline.multiplexer->GetTypes());

			adaptive_union_chunk = make_unique<DataChunk>();
			adaptive_union_chunk->Initialize(Allocator::Get(context.client), pipeline.joins.back()->GetTypes());
			adaptive_union_state = pipeline.adaptive_union->GetOperatorState(context);
		}
	}

	D_ASSERT(pipeline.source_state);
	local_source_state = pipeline.source->GetLocalSourceState(context, *pipeline.source_state);
	if (pipeline.sink) {
		local_sink_state = pipeline.sink->GetLocalSinkState(context);
		requires_batch_index = pipeline.sink->RequiresBatchIndex() && pipeline.source->SupportsBatchIndex();
	}
	bool can_cache_in_pipeline = pipeline.sink && !pipeline.IsOrderDependent() && !requires_batch_index;
	intermediate_chunks.reserve(pipeline.operators.size());
	intermediate_states.reserve(pipeline.operators.size());
	cached_chunks.resize(pipeline.operators.size());
	for (idx_t i = 0; i < pipeline.operators.size(); i++) {
		auto prev_operator = i == 0 ? pipeline.source : pipeline.operators[i - 1];
		auto current_operator = pipeline.operators[i];
		auto chunk = make_unique<DataChunk>();

		if (prev_operator->type == PhysicalOperatorType::MULTIPLEXER) {
			chunk->Initialize(Allocator::Get(context.client), pipeline.adaptive_union->GetTypes());
		} else {
			chunk->Initialize(Allocator::Get(context.client), prev_operator->GetTypes());
		}

		intermediate_chunks.push_back(move(chunk));
		intermediate_states.push_back(current_operator->GetOperatorState(context));

		if (i == pipeline.multiplexer_idx) {
			multiplexer_state = &*intermediate_states.back();
		}

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
	InitializeChunk(final_chunk);
}

bool PipelineExecutor::Execute(idx_t max_chunks) {
	D_ASSERT(pipeline.sink);
	bool exhausted_source = false;
	auto &source_chunk = pipeline.operators.empty() ? final_chunk : *intermediate_chunks[0];
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

#ifdef DEBUG
	if (pipeline.multiplexer) {
		pipeline.multiplexer->PrintStatistics(*multiplexer_state);
		std::ofstream file;
		file.open(std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".csv");
		pipeline.multiplexer->WriteLogToFile(*multiplexer_state, file);
		file.close();
	}
#endif
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
		if (!pipeline.operators.empty()) {
			final_chunk.Reset();
			result = Execute(input, final_chunk, initial_idx);
			if (result == OperatorResultType::FINISHED) {
				return OperatorResultType::FINISHED;
			}
		} else {
			result = OperatorResultType::NEED_MORE_INPUT;
		}
		auto &sink_chunk = pipeline.operators.empty() ? input : final_chunk;
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
	for (idx_t i = 0; i < intermediate_states.size(); i++) {
		intermediate_states[i]->Finalize(pipeline.operators[i], context);
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

void PipelineExecutor::CacheChunk(DataChunk &current_chunk, idx_t operator_idx, bool is_polr_join) {
#if STANDARD_VECTOR_SIZE >= 128
	DataChunk *chunk_cache = nullptr;

	if (is_polr_join && cached_join_chunks[operator_idx]) {
		chunk_cache = &*cached_join_chunks[operator_idx];
	} else if (cached_chunks[operator_idx]) {
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
		auto &source_chunk = pipeline.operators.empty() ? result : *intermediate_chunks[0];
		while (result.size() == 0) {
			if (in_process_operators.empty()) {
				source_chunk.Reset();
				FetchFromSource(source_chunk);
				if (source_chunk.size() == 0) {
					break;
				}
			}
			if (!pipeline.operators.empty()) {
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
	D_ASSERT(!pipeline.operators.empty());

	if (!pipeline.join_paths.empty()) {
		void;
	}

	idx_t current_idx;
	GoToSource(current_idx, initial_idx);
	if (current_idx == initial_idx) {
		current_idx++;
	}

	bool still_processed_path = false;

	if (!in_process_joins.empty()) {
		still_processed_path = true;
		idx_t polr_output_chunk_idx = pipeline.multiplexer_idx + 1;
		auto &output_chunk =
		    polr_output_chunk_idx >= intermediate_chunks.size() ? result : *intermediate_chunks[polr_output_chunk_idx];
		RunPath(*mpx_output_chunk, output_chunk, -1);
		current_idx = polr_output_chunk_idx;

		if (result.size() > 0) {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
	}

	// TODO: what if in_process_joins not empty here, we don't want to go on with cached chunks. Also not if we have
	// output
	for (idx_t i = 0; i < cached_join_chunks.size(); i++) {
		auto &cached_chunk = cached_join_chunks[i];

		if (cached_chunk->size() > 0) {
			still_processed_path = true;
			idx_t polr_output_chunk_idx = pipeline.multiplexer_idx + 1;
			auto &output_chunk = polr_output_chunk_idx >= intermediate_chunks.size()
			                         ? result
			                         : *intermediate_chunks[polr_output_chunk_idx];
			RunPath(*cached_chunk, output_chunk, i + 1);
			cached_chunk->Reset();
			current_idx = polr_output_chunk_idx;
		}
	}

	if (still_processed_path && in_process_joins.empty()) {
		return OperatorResultType::NEED_MORE_INPUT;
	}

	if (result.size() > 0) {
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}

	if (current_idx > pipeline.operators.size()) {
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

			auto current_operator = pipeline.operators[operator_idx];

			if (current_operator->type == PhysicalOperatorType::MULTIPLEXER) {
				mpx_output_chunk->Reset();
				StartOperator(current_operator);
				auto result =
				    current_operator->Execute(context, *prev_chunk, *mpx_output_chunk, *current_operator->op_state,
				                              *intermediate_states[current_intermediate - 1]);
				EndOperator(current_operator, &*mpx_output_chunk);
				if (result == OperatorResultType::HAVE_MORE_OUTPUT) {
					// more data remains in this operator
					// push in-process marker
					in_process_operators.push(current_idx);
				}

				RunPath(*mpx_output_chunk, current_chunk);
				current_chunk.Verify();

				if (current_chunk.size() == 0) {
					// TODO: Is it possible that we have in process joins at this point?
					D_ASSERT(in_process_joins.empty());

					for (idx_t i = 0; i < cached_join_chunks.size(); i++) {
						if (cached_join_chunks[i]->size() > 0) {
							RunPath(*cached_join_chunks[i], current_chunk, i + 1);
							cached_join_chunks[i]->Reset();

							if (current_chunk.size() > 0) {
								break;
							}
						}
					}
				}
			} else {
				// if current_idx > source_idx, we pass the previous' operators output through the Execute of the
				// current operator
				StartOperator(current_operator);
				auto result =
				    current_operator->Execute(context, *prev_chunk, current_chunk, *current_operator->op_state,
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
			if (current_idx > pipeline.operators.size()) {
				// if we got output and are at the last operator, we are finished executing for this output chunk
				// return the data and push it into the chunk
				break;
			}
		}
	}

	bool path_ran_through = true;

	if (!in_process_joins.empty()) {
		path_ran_through = false;
	}

	for (auto &join_cache : cached_join_chunks) {
		if (join_cache->size() > 0) {
			path_ran_through = false;
		}
	}

	return in_process_operators.empty() && path_ran_through ? OperatorResultType::NEED_MORE_INPUT
	                                                        : OperatorResultType::HAVE_MORE_OUTPUT;
}

void PipelineExecutor::FetchFromSource(DataChunk &result) {
	StartOperator(pipeline.source);
	pipeline.source->GetData(context, result, *pipeline.source_state, *local_source_state);
	if (result.size() != 0 && requires_batch_index) {
		auto next_batch_index =
		    pipeline.source->GetBatchIndex(context, result, *pipeline.source_state, *local_source_state);
		next_batch_index += pipeline.base_batch_index;
		D_ASSERT(local_sink_state->batch_index <= next_batch_index ||
		         local_sink_state->batch_index == DConstants::INVALID_INDEX);
		local_sink_state->batch_index = next_batch_index;
	}
	EndOperator(pipeline.source, &result);
}

void PipelineExecutor::InitializeChunk(DataChunk &chunk) {
	PhysicalOperator *last_op = pipeline.operators.empty() ? pipeline.source : pipeline.operators.back();

	if (last_op->type == PhysicalOperatorType::MULTIPLEXER) {
		last_op = &*pipeline.adaptive_union;
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

void PipelineExecutor::RunPath(DataChunk &chunk, DataChunk &result, idx_t start_idx) {
	idx_t current_path = pipeline.multiplexer->GetCurrentPathIndex(*multiplexer_state);

	context.thread.current_join_path = &pipeline.join_paths[current_path];

	if (start_idx == pipeline.joins.size()) {
		if (!in_process_joins.empty()) {
			void;
		}

		D_ASSERT(in_process_joins.empty()); // TODO: this fails

		StartOperator(&*pipeline.adaptive_union);
		pipeline.adaptive_union->Execute(context, chunk, result, *pipeline.adaptive_union->op_state,
		                                 *adaptive_union_state);
		EndOperator(&*pipeline.adaptive_union, &result);
		return;
	}

	if (start_idx == 0 && in_process_joins.empty()) {
		for (idx_t i = 0; i < cached_join_chunks.size(); i++) {
			cached_join_chunks[i]->Destroy();
			cached_join_chunks[i]->Initialize(Allocator::Get(context.client),
			                                  join_intermediate_chunks[current_path][i]->GetTypes());
		}
	}

	idx_t local_join_idx = start_idx;

	if (!in_process_joins.empty()) {
		local_join_idx = in_process_joins.top();
		in_process_joins.pop();
	}

	while (true) {
		auto *prev_chunk = local_join_idx == 0 ? &chunk : &*join_intermediate_chunks[current_path][local_join_idx - 1];
		auto &current_chunk = *join_intermediate_chunks[current_path][local_join_idx];
		current_chunk.Reset();

		idx_t global_join_idx = pipeline.join_paths[current_path][local_join_idx];
		auto current_operator = pipeline.joins[global_join_idx];
		auto &current_state = join_intermediate_states[current_path][local_join_idx];

		StartOperator(current_operator);
		auto join_result =
		    current_operator->Execute(context, *prev_chunk, current_chunk, *current_operator->op_state, *current_state);
		EndOperator(current_operator, &current_chunk);

		pipeline.multiplexer->AddNumIntermediates(*multiplexer_state, current_chunk.size());

		current_chunk.Verify();
		CacheChunk(current_chunk, local_join_idx, true);

		if (join_result == OperatorResultType::HAVE_MORE_OUTPUT) {
			// more data remains in this operator
			// push in-process marker
			in_process_joins.push(local_join_idx);
		}

		if (current_chunk.size() == 0) {
			// no output from this operator!
			if (!in_process_joins.empty()) {
				local_join_idx = in_process_joins.top();
				in_process_joins.pop();
				continue;
			} else {
				break;
			}
		} else {
			// we got output! continue to the next operator
			local_join_idx++;
			if (local_join_idx >= pipeline.joins.size()) {
				StartOperator(&*pipeline.adaptive_union);
				pipeline.adaptive_union->Execute(context, current_chunk, result, *pipeline.adaptive_union->op_state,
				                                 *adaptive_union_state);
				EndOperator(&*pipeline.adaptive_union, &result);
				break;
			}
		}
	}
}

} // namespace duckdb
