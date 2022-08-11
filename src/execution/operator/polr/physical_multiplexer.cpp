#include "duckdb/execution/operator/polr/physical_multiplexer.hpp"

#include <algorithm>
#include <chrono>
#include <map>
#include <iostream>

namespace duckdb {

PhysicalMultiplexer::PhysicalMultiplexer(vector<LogicalType> types, idx_t estimated_cardinality, idx_t path_count_p)
    : PhysicalOperator(PhysicalOperatorType::MULTIPLEXER, move(types), estimated_cardinality),
      path_count(path_count_p) {
}

class MultiplexerState : public OperatorState {
public:
	MultiplexerState(idx_t path_count) {
		intermediates_per_input_tuple.resize(path_count);
		input_tuple_count_per_path.resize(path_count);
	}

	idx_t num_paths_initialized = 0;

	// To be set before running the path
	idx_t chunk_offset = 0;
	idx_t current_path_idx;
	idx_t current_path_tuple_count = 0;

	// To be set after running the path
	vector<double> intermediates_per_input_tuple;
	idx_t num_tuples_processed = 0;
	vector<idx_t> input_tuple_count_per_path;

	// If we only emitted a slice of the current chunk, we set this offset
	const double regret_budget = 0.2;
	const idx_t init_tuple_count = 8;

public:
	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
	}
};

unique_ptr<OperatorState> PhysicalMultiplexer::GetOperatorState(ClientContext &context) const {
	return make_unique<MultiplexerState>(path_count);
}

OperatorResultType PhysicalMultiplexer::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;

	// Initialize each path with one tuple to get initial weights
	if (state.num_paths_initialized < path_count) {
		idx_t next_path_idx = state.num_paths_initialized;
		idx_t remaining_input_tuples = input.size() - state.chunk_offset;
		idx_t tuple_count =
		    remaining_input_tuples > state.init_tuple_count ? state.init_tuple_count : remaining_input_tuples;

		SelectionVector sel(tuple_count);
		for (idx_t i = 0; i < tuple_count; i++) {
			sel.set_index(i, state.chunk_offset + i);
		}
		chunk.Slice(input, sel, tuple_count);

		state.num_paths_initialized++;
		state.chunk_offset += tuple_count;
		state.current_path_idx = next_path_idx;
		state.current_path_tuple_count = tuple_count;

		if (state.chunk_offset == input.size()) {
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			return OperatorResultType::HAVE_MORE_OUTPUT;
		}
	}

	// Order ticks_per_tuples ascending by emplacing them in an (ordered) map, with the time per tuple as key
	// and path index as value
	std::multimap<double, idx_t> sorted_performance_idxs;
	for (idx_t i = 0; i < state.intermediates_per_input_tuple.size(); i++) {
		sorted_performance_idxs.emplace(state.intermediates_per_input_tuple[i], i);
	}
	// Implementation of Bottom-up bounded regret
	// Initialize path weights with 1, so that we can multiply the inits with new path weights
	std::vector<double> path_weights(state.intermediates_per_input_tuple.size(), 1);
	double bottom = sorted_performance_idxs.rbegin()->first;
	for (auto it = std::next(sorted_performance_idxs.rbegin(), 1); it != sorted_performance_idxs.rend(); ++it) {
		// path_weights could be the same, so lets introduce noise
		if (it->first == bottom) {
			bottom += 0.0001;
		}

		double next_bottom = it->first * (1 + state.regret_budget);
		double path_weight_bottom = (it->first - next_bottom) / (it->first - bottom);
		if (path_weight_bottom <= 0) {
			(void) path_weight_bottom;
		}
		D_ASSERT(path_weight_bottom > 0);
		// TODO: this is faulty. Find a way to calculate a weight for a, b, c so that a > b > c
		if (path_weight_bottom > 0.5) {
			path_weight_bottom = 1 - path_weight_bottom;
		}
		// Multiply slower paths backwards with the bottom path weight
		auto it2 = sorted_performance_idxs.rbegin();
		for (it2 = sorted_performance_idxs.rbegin(); it2 != it; it2++) {
			path_weights[it2->second] *= path_weight_bottom;
		}
		// Set weight of the current path
		path_weights[it->second] = 1 - path_weight_bottom;
		bottom = next_bottom;
	}

	idx_t next_path_idx = 0;
	double sent_to_path_ratio = std::numeric_limits<double>::max();
	double max_weight = 0;
	bool next_path_has_max_weight = false;

	for (idx_t i = 0; i < path_weights.size(); ++i) {
		const double current_ratio =
		    (static_cast<double>(state.input_tuple_count_per_path[i]) / state.num_tuples_processed) / path_weights[i];

		if (current_ratio < sent_to_path_ratio) {
			next_path_idx = i;
			sent_to_path_ratio = current_ratio;

			// Let's send more tuples, if we happen to choose the fastest path -> mark it here first
			if (path_weights[i] > max_weight) {
				max_weight = path_weights[i];
				next_path_has_max_weight = true;
			} else {
				next_path_has_max_weight = false;
			}
		}
	}
	idx_t output_tuple_count = 0;
	// We want to process the whole chunk if we are on the fastest path
	if (next_path_has_max_weight) {
		output_tuple_count = input.size() - state.chunk_offset;
	} else {
		// For now, we only send whole chunks
		output_tuple_count = input.size() - state.chunk_offset;
		/* output_tuple_count = std::min(input.size() - state.chunk_offset,
		                              static_cast<idx_t>(state.num_tuples_processed * path_weights[next_path_idx] -
		   state.input_tuple_count_per_path[next_path_idx]));
		*/
	}

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

string PhysicalMultiplexer::ParamsToString() const {
	return "";
}

void PhysicalMultiplexer::FinalizePathRun(OperatorState &state_p, idx_t num_intermediates) const {
	auto &state = (MultiplexerState &)state_p;

	// TODO: intermediates_per_input_tuple can be 0. is there a better way than +1?
	double intermediates_per_input_tuple =
	    (num_intermediates + 1) / static_cast<double>(state.current_path_tuple_count);

	if (state.num_paths_initialized == path_count) {
		// Rolling average
		state.intermediates_per_input_tuple[state.current_path_idx] *= 0.5;
		state.intermediates_per_input_tuple[state.current_path_idx] += 0.5 * intermediates_per_input_tuple;
	} else {
		state.intermediates_per_input_tuple[state.current_path_idx] = intermediates_per_input_tuple;
	}

	state.input_tuple_count_per_path[state.current_path_idx] += state.current_path_tuple_count;
	state.num_tuples_processed += state.current_path_tuple_count;
}

idx_t PhysicalMultiplexer::GetCurrentPathIndex(OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;
	return state.current_path_idx;
}

void PhysicalMultiplexer::PrintStatistics(OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;
	std::cout << "Input tuple counts per path\n";
	for (idx_t i = 0; i < state.input_tuple_count_per_path.size(); i++) {
		std::cout << i << ": " << state.input_tuple_count_per_path[i] << "\n";
	}
}

} // namespace duckdb
