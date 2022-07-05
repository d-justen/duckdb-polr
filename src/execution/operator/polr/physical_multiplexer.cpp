#include "duckdb/execution/operator/polr/physical_multiplexer.hpp"

#include "duckdb/parallel/thread_context.hpp"

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
		ticks_per_tuple.resize(path_count);
		input_tuple_count_per_path.resize(path_count);
		path_weights = vector<double>(path_count, 1.0 / path_count);
		ns_per_tuple = vector<double>(path_count, -1);
	}

	idx_t num_paths_initialized = 0;

	// To be set before running the path
	idx_t chunk_offset = 0;
	idx_t current_path_idx = -1;
	idx_t current_path_tuple_count = -1;
	std::chrono::time_point<std::chrono::system_clock> current_path_begin;

	// To be set after running the path
	vector<double> ticks_per_tuple;
	idx_t num_tuples_processed = 0;
	vector<idx_t> input_tuple_count_per_path;

	// TODO: Do we even need these?
	// Path weights sum should be = 1
	vector<double> path_weights;
	vector<double> ns_per_tuple;

	// If we only emitted a slice of the current chunk, we set this offset
	const double regret_budget = 0.2;
	const idx_t init_tuple_count = 1;

public:
	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
	}
};

unique_ptr<OperatorState> PhysicalMultiplexer::GetOperatorState(ClientContext &context) const {
	return make_unique<MultiplexerState>(path_count);
}

OperatorResultType PhysicalMultiplexer::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	std::cout << "Starting multiplexer..." << std::endl;
	auto &state = (MultiplexerState &)state_p;

	// Initialize each path with one tuple to get initial weights
	if (state.num_paths_initialized < path_count) {
		idx_t next_path_idx = state.num_paths_initialized;

				std::cout << "Initializing path " << next_path_idx << std::endl;

		        idx_t remaining_input_tuples = input.size() - state.chunk_offset;
		        idx_t tuple_count = remaining_input_tuples > state.init_tuple_count ? state.init_tuple_count : remaining_input_tuples;

		        SelectionVector sel(tuple_count);
		        for (idx_t i = 0; i < tuple_count; i++) {
			        sel.set_index(i, state.chunk_offset + i);
		        }
				chunk.Slice(input, sel, tuple_count);

		        state.num_paths_initialized++;
				state.chunk_offset += tuple_count;
		        state.current_path_idx = next_path_idx;
		        state.current_path_tuple_count = tuple_count;
				state.current_path_begin = std::chrono::system_clock::now();

				if (state.chunk_offset == input.size()) {
					return OperatorResultType::NEED_MORE_INPUT;
				} else {
					return OperatorResultType::HAVE_MORE_OUTPUT;
				}
	}

	// Order ticks_per_tuples ascending by emplacing them in an (ordered) map, with the time per tuple as key
	// and path index as value
	std::map<double, idx_t> sorted_performance_idxs;
	for (idx_t i = 0; i < state.ticks_per_tuple.size(); i++) {
		sorted_performance_idxs.emplace(state.ticks_per_tuple[i], i);
	}
	// Implementation of Bottom-up bounded regret
	// Initialize path weights with 1, so that we can multiply the inits with new path weights
	std::vector<double> path_weights(state.ticks_per_tuple.size(), 1);
	double bottom = sorted_performance_idxs.rbegin()->first;
	for (auto it = std::next(sorted_performance_idxs.rbegin(), 1); it != sorted_performance_idxs.rend(); ++it) {
		double next_bottom = it->first * (1 + state.regret_budget);
		double path_weight_bottom = (it->first - next_bottom) / (it->first - bottom);
		// Multiply slower paths backwards with the bottom path weight
		auto it2 = sorted_performance_idxs.rbegin();
		for (it2 = sorted_performance_idxs.rbegin(); it2 != it; it2++) {
			path_weights[it2->second] *= path_weight_bottom;
		}
		// Set weight of the current path
		path_weights[it->second] = 1 - path_weight_bottom;
		bottom = next_bottom;
	}

	idx_t next_path_idx = -1;
	double sent_to_path_ratio = std::numeric_limits<double>::max();
	double max_weight = 0;
	bool next_path_has_max_weight = false;

	// TODO Show ratios

	for (idx_t i = 0; i < path_weights.size(); ++i) {
		const double current_ratio = (static_cast<double>(state.input_tuple_count_per_path[i]) / state.num_tuples_processed) / path_weights[i];
		std::cout << "path ratio" << i << ": " << current_ratio << "\t|\t";
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

	std::cout << std::endl;

	idx_t output_tuple_count = 0;
	// We want to process the whole chunk if we are on the fastest path
	if (next_path_has_max_weight) {
		output_tuple_count = input.size() - state.chunk_offset;
	} else {
		output_tuple_count = std::min(input.size() - state.chunk_offset,
		                              static_cast<idx_t>(state.num_tuples_processed * path_weights[next_path_idx] - state.input_tuple_count_per_path[next_path_idx]));
	}

	if (input.size() - state.chunk_offset > 0) {
		SelectionVector sel;
		sel.Initialize(output_tuple_count);

		for (idx_t i = 0; i < output_tuple_count; i++) {
			sel.set_index(i, state.chunk_offset + i);
		}

		chunk.Slice(input, sel, output_tuple_count);
	}

	state.current_path_idx = next_path_idx;
	state.current_path_tuple_count = output_tuple_count;
	state.current_path_begin = std::chrono::system_clock::now();

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

void PhysicalMultiplexer::FinalizePathRun(OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;

	const auto now = std::chrono::system_clock::now();
	const auto processing_duration = (now - state.current_path_begin).count();
	state.ticks_per_tuple[state.current_path_idx] = processing_duration / static_cast<double>(state.current_path_tuple_count);

	state.input_tuple_count_per_path[state.current_path_idx] += state.current_path_tuple_count;
	state.num_tuples_processed += state.current_path_tuple_count;
}

idx_t PhysicalMultiplexer::GetCurrentPathIndex(OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;
	return state.current_path_idx;
}

} // namespace duckdb
