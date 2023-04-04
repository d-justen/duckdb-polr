#include "duckdb/execution/operator/polr/physical_multiplexer.hpp"

#include "duckdb/main/client_context.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <map>
#include <iostream>
#include <fstream>
#include <queue>

namespace duckdb {

PhysicalMultiplexer::PhysicalMultiplexer(vector<LogicalType> types, idx_t estimated_cardinality, idx_t path_count_p,
                                         double regret_budget_p)
    : PhysicalOperator(PhysicalOperatorType::MULTIPLEXER, move(types), estimated_cardinality), path_count(path_count_p),
      regret_budget(regret_budget_p) {
}

struct HistoryEntry {
	idx_t path_idx;
	idx_t input_tuple_count;
};

class MultiplexerState : public OperatorState {
public:
	MultiplexerState(idx_t path_count) {
		intermediates_per_input_tuple.resize(path_count);
		input_tuple_count_per_path.resize(path_count);
		window_tuples_routed.resize(path_count);
		window_size = path_count * 10;
	}

	bool first_mpx_run = true;
	idx_t num_paths_initialized = 0;

	// To be set before running the path
	idx_t chunk_offset = 0;
	idx_t current_path_idx = 0;
	idx_t current_path_tuple_count = 0;
	idx_t current_path_remaining_tuples = 0;

	// To be set during path run
	idx_t num_intermediates_current_path = 0;

	// To be set after running the path
	vector<double> intermediates_per_input_tuple;
	idx_t num_tuples_processed = 0;
	vector<idx_t> input_tuple_count_per_path;

	const idx_t init_tuple_count = 8;
	std::stringstream log;
	vector<vector<idx_t>> intermediates_alternate_mode;

	idx_t window_size = 20;
	const double smoothing_factor = 0.5;

	std::queue<HistoryEntry> window_history;
	std::vector<idx_t> window_tuples_routed;
	idx_t window_tuples_routed_count = 0;

public:
	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
	}
};

unique_ptr<OperatorState> PhysicalMultiplexer::GetOperatorState(ExecutionContext &context) const {
	return make_unique<MultiplexerState>(path_count);
}

OperatorResultType PhysicalMultiplexer::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;

	if (!state.first_mpx_run) {
		FinalizePathRun(state, context.client.config.log_tuples_routed);
		state.num_intermediates_current_path = 0;
	} else {
		state.first_mpx_run = false;
		if (context.client.config.mpx_alternate_chunks) {
			state.intermediates_alternate_mode = vector<vector<idx_t>>(state.intermediates_per_input_tuple.size());
		}
	}

	if (context.client.config.mpx_alternate_chunks) {
		state.current_path_idx =
		    state.num_tuples_processed == 0 ? 0 : ++state.current_path_idx % state.input_tuple_count_per_path.size();
		state.current_path_tuple_count = input.size();
		chunk.Reference(input);

		if (state.current_path_idx == state.input_tuple_count_per_path.size() - 1) {
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
	}

	// Shortcut: If we have tuples left from previous multiplexing, just route the whole next chunk
	if (state.current_path_remaining_tuples > 0) {
		chunk.Reference(input);
		state.current_path_tuple_count = input.size();

		if (state.current_path_remaining_tuples > input.size()) {
			state.current_path_remaining_tuples -= input.size();
		} else {
			state.current_path_remaining_tuples = 0;
		}

		return OperatorResultType::NEED_MORE_INPUT;
	}

	// Initialize each path with one tuple to get initial weights
	if (state.num_paths_initialized < path_count) {
		idx_t next_path_idx = state.num_paths_initialized;
		D_ASSERT(input.size() > state.chunk_offset);
		idx_t remaining_input_tuples = input.size() - state.chunk_offset;
		idx_t tuple_count =
		    remaining_input_tuples > state.init_tuple_count ? state.init_tuple_count : remaining_input_tuples;

		SelectionVector sel(tuple_count);
		for (idx_t i = 0; i < tuple_count; i++) {
			sel.set_index(i, state.chunk_offset + i);
		}
		chunk.Slice(input, sel, tuple_count);

		state.chunk_offset += tuple_count;
		state.current_path_idx = next_path_idx;
		state.current_path_tuple_count = tuple_count;

		if (state.chunk_offset == input.size()) {
			state.chunk_offset = 0;
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
	}

	vector<double> path_weights;
	CalculateJoinPathWeights(state.intermediates_per_input_tuple, path_weights);

	idx_t next_path_idx = 0;
	double sent_to_path_ratio = std::numeric_limits<double>::max();
	double min_intermediates = std::numeric_limits<double>::max();
	bool next_path_has_min_intermediates = false;

	for (idx_t i = 0; i < path_weights.size(); ++i) {
		const double current_ratio =
		    (static_cast<double>(state.window_tuples_routed[i]) / state.window_tuples_routed_count) / path_weights[i];

		if (current_ratio < sent_to_path_ratio) {
			next_path_idx = i;
			sent_to_path_ratio = current_ratio;

			// Let's send more tuples, if we happen to choose the fastest path -> mark it here first
			if (state.intermediates_per_input_tuple[i] <= min_intermediates) {
				min_intermediates = state.intermediates_per_input_tuple[i];
				next_path_has_min_intermediates = true;
			} else {
				next_path_has_min_intermediates = false;
			}
		}
	}

	auto output_tuple_count = static_cast<int64_t>(std::ceil(
	    state.window_tuples_routed_count * path_weights[next_path_idx] - state.window_tuples_routed[next_path_idx]));

	D_ASSERT(input.size() > state.chunk_offset);
	int64_t remaining_input_tuples = input.size() - state.chunk_offset;
	D_ASSERT(remaining_input_tuples > 0);
	D_ASSERT(remaining_input_tuples <= STANDARD_VECTOR_SIZE);

	if (output_tuple_count > remaining_input_tuples) {
		state.current_path_remaining_tuples = output_tuple_count - remaining_input_tuples;
		output_tuple_count = remaining_input_tuples;

		// We want to process the whole chunk if we are on the fastest path
	} else if (next_path_has_min_intermediates) {
		output_tuple_count = remaining_input_tuples;
	}

	D_ASSERT(output_tuple_count > 0 && output_tuple_count <= 1024);

	if (state.chunk_offset == 0 && output_tuple_count == input.size()) {
		chunk.Reference(input);
	} else if (input.size() - state.chunk_offset > 0) {
		SelectionVector sel;
		sel.Initialize(output_tuple_count);

		for (idx_t i = 0; i < output_tuple_count; i++) {
			sel.set_index(i, state.chunk_offset + i);
		}

		chunk.Slice(input, sel, output_tuple_count);
	}

	state.current_path_idx = next_path_idx;
	state.current_path_tuple_count = output_tuple_count;

	if (input.size() - state.chunk_offset - output_tuple_count > 0) {
		state.chunk_offset += output_tuple_count;
		return OperatorResultType::HAVE_MORE_OUTPUT;
	} else {
		state.chunk_offset = 0;
		return OperatorResultType::NEED_MORE_INPUT;
	}
}

void PhysicalMultiplexer::CalculateJoinPathWeights(const vector<double> &join_path_costs,
                                                   vector<double> &path_weights) const {
	// Order join path costs ascending by emplacing them in an (ordered) map, with the time per tuple as key
	// and path index as value
	std::multimap<double, idx_t> sorted_performance_idxs;
	for (idx_t i = 0; i < join_path_costs.size(); i++) {
		sorted_performance_idxs.emplace(join_path_costs[i], i);
	}

	// Implementation of Bottom-up bounded regret
	// Initialize path weights with 1, so that we can multiply the inits with new path weights
	path_weights.resize(join_path_costs.size(), 1);

	double cost_bottom = sorted_performance_idxs.rbegin()->first;
	for (auto it = std::next(sorted_performance_idxs.rbegin(), 1); it != sorted_performance_idxs.rend(); ++it) {
		const double cost_next = it->first;

		double cost_next_rounded = std::round(cost_next / 0.001) * 0.001;
		double cost_bottom_rounded = std::round(cost_bottom / 0.001) * 0.001;
		// path_weights could be the same, so lets introduce noise
		if (cost_next_rounded == cost_bottom_rounded) {
			cost_bottom += 0.001;
		}

		double cost_target = cost_next * (1 + regret_budget);

		// The target cost (cost_a * (1 + regret_budget)) must be lower than (cost_a + cost_b) / 2 so
		// that path_a gets a lower weight assigned than path_b
		double cost_avg = (cost_next + cost_bottom) / 2;
		if (cost_target >= cost_avg) {
			cost_target = 0.6 * cost_next + 0.4 * cost_bottom;
		}

		const double path_weight_bottom = (cost_next - cost_target) / (cost_next - cost_bottom);

		D_ASSERT(path_weight_bottom > 0);
		D_ASSERT(path_weight_bottom < 0.5);

		// Multiply slower paths backwards with the bottom path weight
		auto it2 = sorted_performance_idxs.rbegin();
		for (it2 = sorted_performance_idxs.rbegin(); it2 != it; it2++) {
			const idx_t join_idx = it2->second;
			path_weights[join_idx] *= path_weight_bottom;
		}
		// Set weight of the current path
		const idx_t join_idx = it->second;
		path_weights[join_idx] = 1 - path_weight_bottom;
		cost_bottom = cost_target;
	}
}

string PhysicalMultiplexer::ParamsToString() const {
	return "";
}

// TODO: We introduced a shortcut to not calculate path_weights after each chunk. If we see that the performance
// deteriorates much, set remaining_tuples to 0
void PhysicalMultiplexer::FinalizePathRun(OperatorState &state_p, bool log_tuples_routed) const {
	auto &state = (MultiplexerState &)state_p;

	if (!state.intermediates_alternate_mode.empty()) {
		state.intermediates_alternate_mode[state.current_path_idx].push_back(state.num_intermediates_current_path);
		state.num_tuples_processed += state.current_path_tuple_count;

		return;
	}

	// If there are no intermediates, we want to add a very small number so that we don't have to process 0s in the
	// weight calculation
	double constant_overhead = 0.1;
	double intermediates_per_input_tuple = (state.num_intermediates_current_path + constant_overhead) /
	                                       static_cast<double>(state.current_path_tuple_count);

	if (state.num_paths_initialized == path_count) {
		if (state.intermediates_per_input_tuple[state.current_path_idx] * 1.5 < intermediates_per_input_tuple) {
			// Our path is now 50% (<- magic number) slower than before. We want to calculate weights again next time
			state.current_path_remaining_tuples = 0;

			// TODO: Is this a viable strategy?
			// TODO: Also use this strategy when logging for efficacy experiment
			// If our go-to path (most tuples were routed there) has seen a deterioration, reset input tuple counts.
			// After this reset, we will be able to revisit that path fairly, in case it will perform better again.
			if (/*!log_tuples_routed*/ false) {
				if (std::none_of(state.input_tuple_count_per_path.cbegin(), state.input_tuple_count_per_path.cend(),
				                 [&](const idx_t tuples) {
					                 return tuples > state.input_tuple_count_per_path[state.current_path_idx];
				                 })) {
					for (idx_t i = 0; i < state.input_tuple_count_per_path.size(); i++) {
						state.input_tuple_count_per_path[i] = 1;
					}
					state.num_tuples_processed = state.input_tuple_count_per_path.size();
				}
			}
		}

		// Rolling average
		state.intermediates_per_input_tuple[state.current_path_idx] *= state.smoothing_factor;
		state.intermediates_per_input_tuple[state.current_path_idx] +=
		    (1 - state.smoothing_factor) * intermediates_per_input_tuple;

		/*if (log_tuples_routed) {
		    state.intermediates_per_input_tuple[state.current_path_idx] = intermediates_per_input_tuple;
		}*/
	} else {
		state.intermediates_per_input_tuple[state.current_path_idx] = intermediates_per_input_tuple;
		state.num_paths_initialized++;
	}

	state.window_history.push(HistoryEntry {state.current_path_idx, state.current_path_tuple_count});
	state.window_tuples_routed[state.current_path_idx] += state.current_path_tuple_count;
	state.window_tuples_routed_count += state.current_path_tuple_count;

	if (state.window_history.size() == state.window_size) {
		auto &entry = state.window_history.front();
		state.window_tuples_routed[entry.path_idx] -= entry.input_tuple_count;
		state.window_tuples_routed_count -= entry.input_tuple_count;
		state.window_history.pop();
	}

	state.input_tuple_count_per_path[state.current_path_idx] += state.current_path_tuple_count;
	state.num_tuples_processed += state.current_path_tuple_count;

	if (log_tuples_routed) {
		if (state.num_tuples_processed - state.current_path_tuple_count == 0) {
			// First run!
			for (idx_t i = 0; i < state.intermediates_per_input_tuple.size(); i++) {
				state.log << "intermediates_" << i << ",";
				state.log << "sent_" << i << ",";
			}

			state.log << "\n";
		}

		for (idx_t i = 0; i < state.intermediates_per_input_tuple.size(); i++) {
			state.log << state.intermediates_per_input_tuple[i] << ",";
			state.log << state.input_tuple_count_per_path[i] << ",";
		}

		state.log << "\n";
	}
}

idx_t PhysicalMultiplexer::GetCurrentPathIndex(OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;
	return state.current_path_idx;
}

void PhysicalMultiplexer::AddNumIntermediates(OperatorState &state_p, idx_t count) const {
	auto &state = (MultiplexerState &)state_p;
	state.num_intermediates_current_path += count;
}

void PhysicalMultiplexer::PrintStatistics(OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;
	std::cout << "Input tuple counts per path\n";
	for (idx_t i = 0; i < state.input_tuple_count_per_path.size(); i++) {
		std::cout << i << ": " << state.input_tuple_count_per_path[i] << "\n";
	}
}

void PhysicalMultiplexer::WriteLogToFile(OperatorState &state_p, std::ofstream &file) const {
	auto &state = (MultiplexerState &)state_p;
	if (!state.intermediates_alternate_mode.empty()) {
		for (idx_t i = 0; i < state.intermediates_alternate_mode.size(); i++) {
			state.log << "path_" << i << ",";
		}
		state.log << "\n";

		for (idx_t i = 0; i < state.intermediates_alternate_mode.front().size(); i++) {
			for (idx_t j = 0; j < state.intermediates_alternate_mode.size(); j++) {
				state.log << state.intermediates_alternate_mode[j][i] << ",";
			}
			state.log << "\n";
		}
	}

	file << state.log.str();
}

} // namespace duckdb
