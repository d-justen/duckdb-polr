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

	// Maybe this can even be called by the executor after running the path??
	void UpdatePathWeight(idx_t path_idx, idx_t tuple_count) {
		const auto now = std::chrono::system_clock::now();
		path_end_timestamps[path_idx] = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();

		// Only update the number of tuples once
		if (path_begin_timestamps[path_idx] != last_path_run_updated) {
			last_path_run_updated = path_begin_timestamps[path_idx];
			input_tuple_count_per_path[path_idx] += tuple_count;
		}

		ns_per_tuple[path_idx] = (path_end_timestamps[path_idx] - static_cast<double>(path_begin_timestamps[path_idx]))
		                         / input_tuple_count_per_path[path_idx];

		// TODO: Update weights according to POLR formula now AFTER each path has been followed once
		if (all_paths_initialized) {
			std::map<double, idx_t> sorted_performance_idxs;

			for (idx_t i = 0; i < ns_per_tuple.size(); i++) {
				sorted_performance_idxs.emplace(ns_per_tuple[i], i);
			}

			const double best_performance = sorted_performance_idxs.begin()->first;
			const double performance_target = best_performance * (1 + regret_budget);

			for (auto it = sorted_performance_idxs.rbegin(); it != sorted_performance_idxs.rend() - 1; ++it) {
				auto faster_path = it + 1;
				auto slower_path = it;

				double target = faster_path->first * (1 + regret_budget);
				double slower_path_weight = (faster_path->first - target) / (faster_path->first - slower_path->first);
				double faster_path_weight = 1 - slower_path_weight;

				// TODO: Next, update the next faster path with the last two ones combined
				// Save target as last_target outside of the loop
				// Apply old path weight * new path weight on all slower paths
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

	// TODO: Clever way to decide on one path and how many tuples should go there
	// For now we choose the highest weighted path and send as many tuples as possible
	context.thread.current_path_idx =
	    std::max_element(state.path_weights.begin(), state.path_weights.end()) - state.path_weights.begin();
	idx_t output_tuple_count = input.size() - state.chunk_offset;

	bool has_remaining_tuples = state.chunk_offset + output_tuple_count < input.size();
	if (state.chunk_offset == 0 && !has_remaining_tuples) {
		chunk.Reference(input);
	} else {
		// Set to remaining input tuple count if output_tuple_count > remainder
		output_tuple_count = has_remaining_tuples ? output_tuple_count : input.size() - state.chunk_offset;

		SelectionVector sel;
		sel.Initialize(output_tuple_count);

		for (idx_t i = 0; i < output_tuple_count; i++) {
			sel.set_index(i, state.chunk_offset + i);
		}

		chunk.Slice(input, sel, output_tuple_count);
	}

	context.thread.update_path_weight = [&]() {
		state.UpdatePathWeight(context.thread.current_path_idx, context.thread.current_path_input_tuple_count);
	};

	context.thread.current_path_input_tuple_count = output_tuple_count;
	state.path_begin_timestamps[context.thread.current_path_idx] =
	    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

	if (has_remaining_tuples) {
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
