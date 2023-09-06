#include "duckdb/parallel/polar_pipeline_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/main/config.hpp"

#include <chrono>
#include <iostream>
#include <unistd.h>
#include <fstream>

namespace duckdb {

POLARPipelineExecutor::POLARPipelineExecutor(ClientContext &context_p, Pipeline &pipeline_p)
    : PipelineExecutor(context_p, pipeline_p) {
	auto &polar = pipeline.polar_config;
	auto join_paths = pipeline.is_backpressure_pipeline ? vector<vector<idx_t>>(1, *pipeline.backpressure_join_order)
	                                                    : polar->join_paths;
	auto &joins = polar->joins;
	multiplexer_state = &*intermediate_states[polar->multiplexer_idx];

	if (!joins.empty()) {
		join_intermediate_chunks.resize(join_paths.size());
		cached_join_chunks.resize(join_paths.size());

		for (idx_t i = 0; i < join_paths.size(); i++) {
			vector<LogicalType> types;
			types.reserve(joins.back()->GetTypes().size());
			auto &multiplexer_types = polar->multiplexer->GetTypes();
			types.insert(types.cend(), multiplexer_types.cbegin(), multiplexer_types.cend());

			auto &current_join_path = join_paths[i];
			join_intermediate_chunks[i].reserve(current_join_path.size());
			cached_join_chunks[i].reserve(current_join_path.size());

			for (idx_t j = 0; j < current_join_path.size(); j++) {
				auto &build_types = joins[current_join_path[j]]
				                        ->build_types; // TODO: check if this is the new types that come with the join
				types.insert(types.cend(), build_types.cbegin(), build_types.cend());

				join_intermediate_chunks[i].push_back(make_unique<DataChunk>());
				join_intermediate_chunks[i].back()->Initialize(Allocator::Get(context.client), types);

				cached_join_chunks[i].push_back(make_unique<DataChunk>());
				cached_join_chunks[i].back()->Initialize(Allocator::Get(context.client), types);
			}
		}

		join_intermediate_states.reserve(join_paths.size());
		for (idx_t i = 0; i < join_paths.size(); i++) {
			auto &join_path = join_paths[i];
			vector<unique_ptr<OperatorState>> states;
			states.reserve(joins.size());

			for (idx_t j = 0; j < joins.size(); j++) {
				idx_t join_idx = join_path[j];
				auto *join = joins[join_idx];
				auto &bindings = pipeline.is_backpressure_pipeline ? pipeline.polar_bindings[j]
				                                                   : polar->left_expression_bindings[i][j];
				auto state = join->GetOperatorStateWithBindings(context, bindings);

				states.push_back(move(state));

				if (join->sink_state->state == SinkFinalizeType::NO_OUTPUT_POSSIBLE) {
					FinishProcessing();
					polar->log_tuples_routed = false;
					polar->measure_polr_pipeline = false;
				}
			}
			join_intermediate_states.push_back(move(states));
		}

		mpx_output_chunk = make_unique<DataChunk>();
		mpx_output_chunk->Initialize(Allocator::Get(context.client), polar->multiplexer->GetTypes());

		adaptive_union_state = polar->adaptive_union->GetOperatorState(context);
	}
}

bool POLARPipelineExecutor::Execute(idx_t max_chunks) {
	if (!PipelineExecutor::Execute(max_chunks)) {
		return false;
	}

	auto &polar = pipeline.polar_config;

	if (polar->log_tuples_routed) {
		polar->multiplexer->PrintStatistics(*multiplexer_state);
		std::string filename = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
		std::ofstream file;
		string &prefix = DBConfig::GetConfig(pipeline.executor.context).options.dir_prefix;
		char tmp[256];
		getcwd(tmp, 256);
		file.open(std::string(tmp) + "/tmp/" + prefix + filename + ".csv");

		if (!polar->multiplexer->WasExecuted(*multiplexer_state)) {
			auto routing = DBConfig::GetConfig(pipeline.executor.context).options.multiplexer_routing;
			if (routing == MultiplexerRouting::ALTERNATE) {
				file << "path_0\n0\n";
			} else {
				file << "intermediates\n0\n";
			}
		} else {
			polar->multiplexer->WriteLogToFile(*multiplexer_state, file);
		}

		file.close();
		std::cout << filename << "\n";

		std::ofstream file2;
		file2.open(std::string(tmp) + "/tmp/" + prefix + filename + "-intms.txt");
		file2 << num_intermediates_produced << "\n";
		file2.close();
	}

	return true;
}

void POLARPipelineExecutor::PushFinalize() {
	if (finalized) {
		throw InternalException("Calling PushFinalize on a pipeline that has been finalized already");
	}
	finalized = true;
	// flush all caches
	// note that even if an operator has finished, we might still need to flush caches AFTER that operator
	// e.g. if we have SOURCE -> LIMIT -> CROSS_PRODUCT -> SINK, if the LIMIT reports no more rows will be passed on
	// we still need to flush caches from the CROSS_PRODUCT
	D_ASSERT(in_process_operators.empty());
	D_ASSERT(in_process_joins.empty());

	idx_t current_path = pipeline.polar_config->multiplexer->GetCurrentPathIndex(*multiplexer_state);
	for (idx_t i = 0; i < cached_join_chunks[current_path].size(); i++) {
		if (cached_join_chunks[current_path][i] && cached_join_chunks[current_path][i]->size() > 0) {
			ExecutePushInternal(*mpx_output_chunk, pipeline.polar_config->multiplexer_idx);
		}
	}

	for (idx_t i = 0; i < cached_join_chunks.size(); i++) {
		for (idx_t j = 0; j < cached_join_chunks[current_path].size(); j++) {
			D_ASSERT(!cached_join_chunks[i][j] || cached_join_chunks[i][j]->size() == 0);
			if (cached_join_chunks[i][j]) {
				cached_join_chunks[i][j].reset();
			}
		}
	}

	idx_t start_idx = IsFinished() ? idx_t(finished_processing_idx) : 0;
	for (idx_t i = start_idx; i < cached_chunks.size(); i++) {
		if (cached_chunks[i] && cached_chunks[i]->size() > 0) {
			ExecutePushInternal(*cached_chunks[i], i + 1);
			cached_chunks[i].reset();
		}
	}

	D_ASSERT(in_process_operators.empty());
	D_ASSERT(in_process_joins.empty());

	pipeline.polar_config->multiplexer->FinalizePathRun(*multiplexer_state,
	                                                    pipeline.executor.context.config.log_tuples_routed);

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

void POLARPipelineExecutor::CacheJoinChunk(DataChunk &current_chunk, idx_t operator_idx) {
#if STANDARD_VECTOR_SIZE >= 128
	if (!context.client.config.caching) {
		return;
	}

	DataChunk *chunk_cache = nullptr;

	idx_t current_path = pipeline.polar_config->multiplexer->GetCurrentPathIndex(*multiplexer_state);
	if (cached_join_chunks[current_path][operator_idx]) {
		chunk_cache = &*cached_join_chunks[current_path][operator_idx];
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

OperatorResultType POLARPipelineExecutor::FlushInProcessJoins(DataChunk &result, bool &did_flush) {
	if (in_process_joins.empty()) {
		return OperatorResultType::FINISHED;
	}

	if (!in_process_operators.empty()) {
		idx_t next_non_join_idx = in_process_operators.top();
		if (next_non_join_idx > pipeline.polar_config->multiplexer_idx + 1) {
			return OperatorResultType::FINISHED;
		}
	}

	result.Reset();
	RunPath(*mpx_output_chunk, result, -1);
	did_flush = true;

	if (result.size() == 0) {
		D_ASSERT(in_process_joins.empty());
		return OperatorResultType::FINISHED;
	} else if (in_process_joins.empty()) {
		return OperatorResultType::NEED_MORE_INPUT;
	} else {
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}
}

OperatorResultType POLARPipelineExecutor::FlushJoinCaches(DataChunk &result, bool &did_flush) {
	auto &polar_config = pipeline.polar_config;
	idx_t cache_skips_left = polar_config->multiplexer->GetNumCacheFlushingSkips(*multiplexer_state);
	if (cache_skips_left > 0 && !finalized) {
		return OperatorResultType::FINISHED;
	}

	idx_t current_path = polar_config->multiplexer->GetCurrentPathIndex(*multiplexer_state);

	for (idx_t i = 0; i < cached_join_chunks[current_path].size(); i++) {
		auto &cached_chunk = cached_join_chunks[current_path][i];

		if (cached_chunk && cached_chunk->size() > 0) {
			did_flush = true;
			result.Reset();
			RunPath(*cached_chunk, result, i + 1);
			cached_chunk->Reset();

			if (result.size() > 0) {
				for (idx_t j = i; j < cached_join_chunks[current_path].size(); j++) {
					if (cached_join_chunks[current_path][j]->size() > 0) {
						return OperatorResultType::HAVE_MORE_OUTPUT;
					}
				}
				return OperatorResultType::NEED_MORE_INPUT;
			}
		}
	}

	return OperatorResultType::FINISHED;
}

OperatorResultType POLARPipelineExecutor::Execute(DataChunk &input, DataChunk &result, idx_t initial_idx) {
	auto &operators = pipeline.GetOperators();
	D_ASSERT(!operators.empty());

	auto &polar_config = pipeline.polar_config;
	idx_t polr_output_chunk_idx = polar_config->multiplexer_idx + 1;
	auto &output_chunk =
	    polr_output_chunk_idx >= intermediate_chunks.size() ? result : *intermediate_chunks[polr_output_chunk_idx];

	bool did_flush = false;
	auto op_result = FlushInProcessJoins(output_chunk, did_flush);
	if (op_result == OperatorResultType::FINISHED) {
		// Flushing in process joins had no output
		op_result = FlushJoinCaches(output_chunk, did_flush);
	}

	idx_t current_idx;
	if (op_result == OperatorResultType::FINISHED) {
		if (did_flush && in_process_operators.empty()) {
			return OperatorResultType::NEED_MORE_INPUT;
		}
		if (input.size() == 0) { // LCOV_EXCL_START
			return OperatorResultType::NEED_MORE_INPUT;
		} // LCOV_EXCL_STOP

		// Both flushes had no effect
		GoToSource(current_idx, initial_idx);
		if (current_idx == initial_idx) {
			current_idx++;
		}
	} else {
		// Join output! Jump forward to operator after join sequence
		current_idx = polar_config->multiplexer_idx + 2;
		if (current_idx > operators.size()) {
			// No ops follow the joins
			// Return where any of the operators/joins are still in process
			op_result = in_process_operators.empty() ? op_result : OperatorResultType::HAVE_MORE_OUTPUT;
			return op_result;
		}
	}

	idx_t current_path = polar_config->multiplexer->GetCurrentPathIndex(*multiplexer_state);
	idx_t &cache_skips_left = polar_config->multiplexer->GetNumCacheFlushingSkips(*multiplexer_state);

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
			break;
		} else {
			auto *prev_chunk =
			    current_intermediate == initial_idx + 1 ? &input : &*intermediate_chunks[current_intermediate - 1];
			auto operator_idx = current_idx - 1;

			auto current_operator = operators[operator_idx];

			if (current_operator->type == PhysicalOperatorType::MULTIPLEXER) {
				D_ASSERT(in_process_joins.empty());
				if (cache_skips_left > 0) {
					cache_skips_left--;

					mpx_output_chunk->Reset();
					mpx_output_chunk->Reference(*prev_chunk);

					polar_config->multiplexer->IncreaseInputTupleCount(*multiplexer_state, mpx_output_chunk->size());
					RunPath(*mpx_output_chunk, current_chunk);
					current_chunk.Verify();
				} else {
					for (idx_t i = 0; i < cached_join_chunks.size(); i++) {
						for (idx_t j = 0; j < cached_join_chunks[current_path].size(); j++) {
							D_ASSERT(!cached_join_chunks[i][j] || cached_join_chunks[i][j]->size() == 0);
						}
					}

					mpx_output_chunk->Reset();
					StartOperator(current_operator);
					auto result =
					    current_operator->Execute(context, *prev_chunk, *mpx_output_chunk, *current_operator->op_state,
					                              *intermediate_states[current_intermediate - 1]);
					EndOperator(current_operator, &*mpx_output_chunk);
					if (result == OperatorResultType::HAVE_MORE_OUTPUT) {
						// more data remains in this operator
						// push in-process marker
						D_ASSERT(cache_skips_left == 0);
						in_process_operators.push(current_idx);
					}

					current_path = polar_config->multiplexer->GetCurrentPathIndex(*multiplexer_state);
					RunPath(*mpx_output_chunk, current_chunk);
					current_chunk.Verify();

					if (current_chunk.size() == 0) {
						D_ASSERT(in_process_joins.empty());
						// We have no output but still want to return here if we want to clear the join caches
						if (cache_skips_left == 0) {
							for (idx_t i = 0; i < cached_join_chunks[current_path].size(); i++) {
								if (cached_join_chunks[current_path][i] &&
								    cached_join_chunks[current_path][i]->size() > 0) {
									return OperatorResultType::HAVE_MORE_OUTPUT;
								}
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
			if (current_idx > operators.size()) {
				// if we got output and are at the last operator, we are finished executing for this output chunk
				// return the data and push it into the chunk
				break;
			}
		}
	}

	if (!in_process_operators.empty() || !in_process_joins.empty() ||
	    op_result == OperatorResultType::HAVE_MORE_OUTPUT) {
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}

	if (cache_skips_left == 0 || finalized) {
		for (idx_t i = 0; i < cached_join_chunks[current_path].size(); i++) {
			if (cached_join_chunks[current_path][i] && cached_join_chunks[current_path][i]->size() > 0) {
				return OperatorResultType::HAVE_MORE_OUTPUT;
			}
		}
	}

	return OperatorResultType::NEED_MORE_INPUT;
}

void POLARPipelineExecutor::RunPath(DataChunk &chunk, DataChunk &result, idx_t start_idx) {
	const auto &multiplexer = pipeline.polar_config->multiplexer;
	auto &join_paths = pipeline.polar_config->join_paths;
	const auto &joins = pipeline.polar_config->joins;
	const auto &adaptive_union = pipeline.polar_config->adaptive_union;

	idx_t current_path = multiplexer->GetCurrentPathIndex(*multiplexer_state);
	bool running_cache = start_idx != 0 && in_process_joins.empty();
	bool must_rerun_cache = false;

	context.thread.current_join_path =
	    pipeline.is_backpressure_pipeline ? &*pipeline.backpressure_join_order : &join_paths[current_path];
	auto &current_join_path = *context.thread.current_join_path;

	if (start_idx == joins.size()) {
		D_ASSERT(in_process_joins.empty());

		if (multiplexer->routing == MultiplexerRouting::ALTERNATE && current_path != 0) {
			return;
		}

		StartOperator(&*adaptive_union);
		adaptive_union->Execute(context, chunk, result, *adaptive_union->op_state, *adaptive_union_state);
		EndOperator(&*adaptive_union, &result);
		return;
	}

	idx_t local_join_idx = start_idx;

	if (!in_process_joins.empty()) {
		local_join_idx = in_process_joins.top();
		start_idx = 0;
		in_process_joins.pop();
	}

	while (true) {
		auto *prev_chunk =
		    local_join_idx == start_idx ? &chunk : &*join_intermediate_chunks[current_path][local_join_idx - 1];
		auto &current_chunk = *join_intermediate_chunks[current_path][local_join_idx];
		current_chunk.Reset();

		idx_t global_join_idx = current_join_path[local_join_idx];
		auto current_operator = joins[global_join_idx];
		auto &current_state = join_intermediate_states[current_path][local_join_idx];

		StartOperator(current_operator);
		auto join_result =
		    current_operator->Execute(context, *prev_chunk, current_chunk, *current_operator->op_state, *current_state);
		EndOperator(current_operator, &current_chunk);

		if (running_cache && local_join_idx == start_idx) {
			if (join_result == OperatorResultType::HAVE_MORE_OUTPUT) {
				must_rerun_cache = true;
			} else {
				must_rerun_cache = false;
			}
		}

		multiplexer->AddNumIntermediates(*multiplexer_state, current_chunk.size());
		num_intermediates_produced += current_chunk.size();

		current_chunk.Verify();
		CacheJoinChunk(current_chunk, local_join_idx);

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
				// TODO: go on with cached chunks?
				break;
			}
		} else {
			// we got output! continue to the next operator
			local_join_idx++;
			if (local_join_idx >= joins.size()) {
				if (multiplexer->routing == MultiplexerRouting::ALTERNATE && current_path != 0) {
					if (!in_process_joins.empty()) {
						local_join_idx = in_process_joins.top();
						in_process_joins.pop();
						continue;
					} else {
						// TODO: go on with cached chunks?
						break;
					}
				}

				StartOperator(&*adaptive_union);
				adaptive_union->Execute(context, current_chunk, result, *adaptive_union->op_state,
				                        *adaptive_union_state);
				EndOperator(&*adaptive_union, &result);

				if (must_rerun_cache) {
					cached_join_chunks[current_path][start_idx - 1].swap(
					    join_intermediate_chunks[current_path][start_idx - 1]);
				}
				return;
			}
		}
	}
}

} // namespace duckdb
