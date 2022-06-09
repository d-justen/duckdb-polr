#include "duckdb/execution/operator/polr/physical_multiplexer.hpp"

#include "duckdb/parallel/thread_context.hpp"

#include <algorithm>
#include <chrono>
#include <map>

namespace duckdb {

PhysicalMultiplexer::PhysicalMultiplexer(vector<LogicalType> types, idx_t estimated_cardinality, idx_t path_count_p)
    : PhysicalOperator(PhysicalOperatorType::MULTIPLEXER, move(types), estimated_cardinality),
      path_count(path_count_p) {
}

class MultiplexerState : public OperatorState {
public:
	MultiplexerState(idx_t path_count) {
		input_tuple_count_per_path.resize(path_count);
		path_begin_timestamps.resize(path_count);
		path_end_timestamps.resize(path_count);
		path_weights = vector<double>(path_count, 1.0 / path_count);
		ns_per_tuple = vector<double>(path_count, -1);
		chunk_offset = 0;
		last_path_run_updated = 0;
		all_paths_initialized = false;
	}

	vector<idx_t> input_tuple_count_per_path;

	// Note: begin timestamp can be > end. If so, the path has not been fully executed yet.
	vector<idx_t> path_begin_timestamps;
	vector<idx_t> path_end_timestamps;

	idx_t last_path_run_updated;

	// Path weights sum should be = 1
	vector<double> path_weights;
	vector<double> ns_per_tuple;

	// If we only emitted a slice of the current chunk, we set this offset
	idx_t chunk_offset;
	bool all_paths_initialized;
	const double regret_budget = 0.2;

public:
	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
	}

	void UpdatePathWeight(idx_t path_idx, idx_t tuple_count) {
		const auto now = std::chrono::system_clock::now();
		path_end_timestamps[path_idx] = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();

		// UpdatePathWeight might be called multiple times for the same path. Therefore, we only want to set the input
		// tuple count once. However, this breaks if we run a path with multiple chunks, as it doesn't register the
		// new tuples then.
		// FIXME: Make sure UpdatePathWeight gets called exactly once per path run, so that we don't have to deal with this
		if (path_begin_timestamps[path_idx] != last_path_run_updated) {
			last_path_run_updated = path_begin_timestamps[path_idx];
			input_tuple_count_per_path[path_idx] += tuple_count;
		}

		ns_per_tuple[path_idx] = (path_end_timestamps[path_idx] - static_cast<double>(path_begin_timestamps[path_idx]))
		                         / input_tuple_count_per_path[path_idx];

		// After all paths are initialized (i.e. they were run once), we want to update the path weights
		if (all_paths_initialized) {
			// Order ns_per_tuples ascending by emplacing them in an (ordered) map, with the time per tuple as key
			// and path index as value
			std::map<double, idx_t> sorted_performance_idxs;

			sorted_performance_idxs.reserve(ns_per_tuple.size());
			for (idx_t i = 0; i < ns_per_tuple.size(); i++) {
				sorted_performance_idxs.emplace(ns_per_tuple[i], i);
			}

			// Implementation of Bottom-up bounded regret
			// Initialize path weights with 1, so that we can multiply the inits with new path weights
			path_weights = std::vector<double>(ns_per_tuple.size(), 1);
			double bottom = sorted_performance_idxs.rbegin()->first;

			for (auto it = std::next(sorted_performance_idxs.rbegin(), 1); it != sorted_performance_idxs.rend(); ++it) {
				double next_bottom = it->first * (1 + regret_budget);
				double path_weight_bottom = (it->first - next_bottom) / (it->first - bottom);

				// Multiply slower paths backwards with the bottom path weight
				for (auto it2 = std::prev(it, 1); it2 != sorted_performance_idxs.rbegin(); --it2) {
					path_weights[it2->second] *= path_weight_bottom;
				}

				// Set weight of the current path
				path_weights[it->second] = 1 - path_weight_bottom;

				bottom = next_bottom;
			}
		}

	}
};

unique_ptr<OperatorState> PhysicalMultiplexer::GetOperatorState(ClientContext &context) const {
	return make_unique<MultiplexerState>(path_count);
}

OperatorResultType PhysicalMultiplexer::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;

	// Initialize each path with one tuple to get initial weights
	if (!state.all_paths_initialized) {
		for (idx_t i = 0; i < state.path_begin_timestamps.size(); i++) {
			if (state.path_begin_timestamps[i] == 0) {
				context.thread.current_path_idx = i;

				SelectionVector sel(1);
				sel.set_index(0, state.chunk_offset);
				chunk.Slice(input, sel, 1);

				state.chunk_offset++;

				if (i == state.path_begin_timestamps.size() - 1) {
					state.all_paths_initialized = true;
				}

				state.path_begin_timestamps[i] = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

				if (state.chunk_offset == input.size()) {
					return OperatorResultType::NEED_MORE_INPUT;
				} else {
					return OperatorResultType::HAVE_MORE_OUTPUT;
				}
			}
		}
	}

	idx_t processed_tuple_count = std::accumulate(state.input_tuple_count_per_path.cbegin(),
	                                              state.input_tuple_count_per_path.cend(), 0);

	idx_t next_path_idx = -1;
	double sent_to_path_ratio = std::numeric_limits<double>::max();
	double max_weight = 0;
	bool next_path_has_max_weight = false;

	for (idx_t i = 0; i < state.path_weights.size(); ++i) {
		const double current_ratio = (state.input_tuple_count_per_path[i] / processed_tuple_count) / state.path_weights[i];
		if (current_ratio < sent_to_path_ratio) {
			next_path_idx = i;
			sent_to_path_ratio = current_ratio;

			// Let's send more tuples, if we happen to choose the fastest path -> mark it here first
			if (state.path_weights[i] > max_weight) {
				max_weight = state.path_weights[i];
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
		output_tuple_count = std::min(input.size() - state.chunk_offset,
		                              static_cast<idx_t>(processed_tuple_count * state.path_weights[next_path_idx] - state.input_tuple_count_per_path[next_path_idx]));
	}

	if (input.size() - state.chunk_offset > 0) {
		SelectionVector sel;
		sel.Initialize(output_tuple_count);

		for (idx_t i = 0; i < output_tuple_count; i++) {
			sel.set_index(i, state.chunk_offset + i);
		}

		chunk.Slice(input, sel, output_tuple_count);
	}

	context.thread.current_path_idx = next_path_idx;
	context.thread.current_path_input_tuple_count = output_tuple_count;
	context.thread.update_path_weight = [&]() {
		state.UpdatePathWeight(context.thread.current_path_idx, context.thread.current_path_input_tuple_count);
	};

	state.path_begin_timestamps[context.thread.current_path_idx] =
	    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

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

} // namespace duckdb
