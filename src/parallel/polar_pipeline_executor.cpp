#include "duckdb/parallel/polar_pipeline_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/limits.hpp"

#include <chrono>
#include <iostream>
#include <fstream>

namespace duckdb {

POLARPipelineExecutor::POLARPipelineExecutor(ClientContext &context_p, Pipeline &pipeline_p)
    : PipelineExecutor(context_p, pipeline_p) {
	auto &polar = pipeline.polar_config;
	auto &joins = polar->joins;
	auto &join_paths = polar->join_paths;
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
				auto &bindings = polar->left_expression_bindings[i][j];
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
		if (!polar->multiplexer->WasExecuted(*multiplexer_state)) {
			return true;
		}
		polar->multiplexer->PrintStatistics(*multiplexer_state);
		std::string filename = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
		std::ofstream file;
		file.open("./experiments/" + filename + ".csv");
		polar->multiplexer->WriteLogToFile(*multiplexer_state, file);
		file.close();
		std::cout << filename << "\n";

		std::ofstream file2;
		file2.open("./experiments/" + filename + "-intms.txt");
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
	idx_t start_idx = IsFinished() ? idx_t(finished_processing_idx) : 0;
	for (idx_t i = start_idx; i < cached_chunks.size(); i++) {
		if (cached_chunks[i] && cached_chunks[i]->size() > 0) {
			ExecutePushInternal(*cached_chunks[i], i + 1);
			cached_chunks[i].reset();
		}
	}

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

OperatorResultType POLARPipelineExecutor::Execute(DataChunk &input, DataChunk &result, idx_t initial_idx) {
	if (input.size() == 0) { // LCOV_EXCL_START
		return OperatorResultType::NEED_MORE_INPUT;
	} // LCOV_EXCL_STOP
	auto &operators = pipeline.GetOperators();
	D_ASSERT(!operators.empty());

	auto &polar_config = pipeline.polar_config;

	idx_t current_idx;
	idx_t current_path = polar_config ? polar_config->multiplexer->GetCurrentPathIndex(*multiplexer_state) : -1;
	bool did_work_from_prior_run = false;
	bool prior_run_tuples_produced_output = false;

	if (!in_process_joins.empty()) {
		did_work_from_prior_run = true;
		idx_t polr_output_chunk_idx = polar_config->multiplexer_idx + 1;
		auto &output_chunk =
		    polr_output_chunk_idx >= intermediate_chunks.size() ? result : *intermediate_chunks[polr_output_chunk_idx];
		output_chunk.Reset();
		RunPath(*mpx_output_chunk, output_chunk, -1);

		if (result.size() > 0) {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}

		prior_run_tuples_produced_output = output_chunk.size() > 0;
	}

	if (!prior_run_tuples_produced_output) {
		for (idx_t i = 0; i < cached_join_chunks[current_path].size(); i++) {
			auto &cached_chunk = cached_join_chunks[current_path][i];

			if (cached_chunk->size() > 0) {
				did_work_from_prior_run = true;
				idx_t polr_output_chunk_idx = polar_config->multiplexer_idx + 1;
				auto &output_chunk = polr_output_chunk_idx >= intermediate_chunks.size()
				                         ? result
				                         : *intermediate_chunks[polr_output_chunk_idx];
				output_chunk.Reset();
				RunPath(*cached_chunk, output_chunk, i + 1);
				prior_run_tuples_produced_output = output_chunk.size() > 0;
				cached_chunk->Reset();

				if (prior_run_tuples_produced_output) {
					break;
				}
			}
		}
	}

	if (did_work_from_prior_run && !prior_run_tuples_produced_output && in_process_operators.empty()) {
		return OperatorResultType::NEED_MORE_INPUT; // TODO: Do we have to GoToSource instead?
	}

	if (prior_run_tuples_produced_output) {
		current_idx = polar_config->multiplexer_idx + 2;
	}

	if (result.size() > 0) {
		if (in_process_operators.empty() && in_process_joins.empty()) {
			for (auto &chunk : cached_join_chunks[current_path]) {
				if (chunk->size() > 0) {
					return OperatorResultType::HAVE_MORE_OUTPUT;
				}
			}
			return OperatorResultType::NEED_MORE_INPUT;
		}
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}

	if (!prior_run_tuples_produced_output) {
		GoToSource(current_idx, initial_idx);
		if (current_idx == initial_idx) {
			current_idx++;
		}

		if (current_idx > operators.size()) {
			result.Reference(input);
			return OperatorResultType::NEED_MORE_INPUT;
		}
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

			if (current_operator->type == PhysicalOperatorType::MULTIPLEXER) {
				D_ASSERT(in_process_joins.empty());
				for (idx_t i = 0; i < cached_join_chunks[current_path].size(); i++) {
					D_ASSERT(cached_join_chunks[current_path][i]->size() == 0);
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
					in_process_operators.push(current_idx);
				}

				current_path = polar_config->multiplexer->GetCurrentPathIndex(*multiplexer_state);
				RunPath(*mpx_output_chunk, current_chunk);
				current_chunk.Verify();

				if (current_chunk.size() == 0) {
					D_ASSERT(in_process_joins.empty());

					for (idx_t i = 0; i < cached_join_chunks[current_path].size(); i++) {
						if (cached_join_chunks[current_path][i]->size() > 0) {
							RunPath(*cached_join_chunks[current_path][i], current_chunk, i + 1);
							cached_join_chunks[current_path][i]->Reset();

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
				if (!in_process_joins.empty()) {
					return OperatorResultType::HAVE_MORE_OUTPUT;
				}
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

	bool path_ran_through = true;

	if (!in_process_joins.empty()) {
		path_ran_through = false;
	}

	for (auto &join_cache : cached_join_chunks[current_path]) {
		if (join_cache->size() > 0) {
			path_ran_through = false;
		}
	}

	return in_process_operators.empty() && path_ran_through ? OperatorResultType::NEED_MORE_INPUT
	                                                        : OperatorResultType::HAVE_MORE_OUTPUT;
}

void POLARPipelineExecutor::RunPath(DataChunk &chunk, DataChunk &result, idx_t start_idx) {
	const auto &multiplexer = pipeline.polar_config->multiplexer;
	auto &join_paths = pipeline.polar_config->join_paths;
	const auto &joins = pipeline.polar_config->joins;
	const auto &adaptive_union = pipeline.polar_config->adaptive_union;

	idx_t current_path = multiplexer->GetCurrentPathIndex(*multiplexer_state);
	bool running_cache = start_idx != 0 && in_process_joins.empty();
	bool must_rerun_cache = false;

	context.thread.current_join_path = &join_paths[current_path];

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

		idx_t global_join_idx = join_paths[current_path][local_join_idx];
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
