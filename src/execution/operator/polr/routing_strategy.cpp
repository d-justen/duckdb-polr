#include "duckdb/execution/operator/polr/routing_strategy.hpp"

#include <map>

namespace duckdb {

OperatorResultType RoutingStrategy::SelectTuples(DataChunk &input, DataChunk &chunk) const {
	auto &state = *routing_state;

	if (state.next_tuple_count == input.size()) {
		D_ASSERT(state.chunk_offset == 0);
		chunk.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	D_ASSERT(state.chunk_offset + state.next_tuple_count <= input.size());

	auto *sel_vector = state.sel.data();

	for (idx_t i = 0; i < state.next_tuple_count; i++) {
		sel_vector[i] = state.chunk_offset + i;
	}

	chunk.Slice(input, state.sel, state.next_tuple_count);

	if (state.chunk_offset + state.next_tuple_count == input.size()) {
		state.chunk_offset = 0;
		return OperatorResultType::NEED_MORE_INPUT;
	}

	state.chunk_offset += state.next_tuple_count;
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

idx_t OpportunisticRoutingStrategy::DetermineNextPath() const {
	auto &path_resistances = *routing_state->path_resistances;

	double min_resistance = path_resistances.front();
	idx_t next_path_idx = 0;

	for (idx_t i = 1; i < path_resistances.size(); i++) {
		if (path_resistances[i] < min_resistance) {
			min_resistance = path_resistances[i];
			next_path_idx = i;
		}
	}

	return next_path_idx;
}

idx_t OpportunisticRoutingStrategy::DetermineNextTupleCount() const {
	return routing_state->chunk_size;
}

idx_t InitOnceRoutingStrategy::DetermineNextPath() const {
	auto &state = (InitOnceRoutingStrategyState &)*routing_state;

	if (state.init_phase_done) {
		state.num_cache_flushing_skips = std::numeric_limits<idx_t>::max();
		return state.best_path_after_init;
	}

	if (state.num_paths_initialized == state.path_resistances->size()) {
		auto &path_resistances = *state.path_resistances;

		double min_resistance = path_resistances.front();
		idx_t path_idx = 0;

		for (idx_t i = 1; i < path_resistances.size(); i++) {
			if (path_resistances[i] < min_resistance) {
				min_resistance = path_resistances[i];
				path_idx = i;
			}
		}

		state.init_phase_done = true;
		state.best_path_after_init = path_idx;
		return state.best_path_after_init;
	}

	return state.num_paths_initialized++;
}

idx_t InitOnceRoutingStrategy::DetermineNextTupleCount() const {
	auto &state = (InitOnceRoutingStrategyState &)*routing_state;

	if (state.init_phase_done) {
		return state.chunk_size - state.chunk_offset;
	}

	return std::min(init_tuple_count, state.chunk_size - state.chunk_offset);
}

idx_t AdaptiveReinitRoutingStrategy::DetermineNextPath() const {
	auto &state = (AdaptiveReinitRoutingStrategyState &)*routing_state;

	if (state.init_phase_done) {
		auto &path_resistances = *state.path_resistances;

		double min_resistance = path_resistances.front();
		idx_t min_resistance_path_idx = 0;

		for (idx_t i = 1; i < path_resistances.size(); i++) {
			if (path_resistances[i] < min_resistance) {
				min_resistance_path_idx = i;
				min_resistance = path_resistances[i];
			}
		}

		// Fallback on original path if optimal path has around the same resistance (<= +5%)
		if (min_resistance * 1.05 >= path_resistances[0]) {
			min_resistance = path_resistances[0];
			min_resistance_path_idx = 0;
		}

		if (state.window_offset == 0 || !state.visited_paths[min_resistance_path_idx]) {
			// After (re-) initialization, we are about to send the first chunk to the path of least resistance
			// Find the next path and estimate the cost for the next initialization
			state.visited_paths[min_resistance_path_idx] = true;

			double reinit_cost_estimate = 0;
				for (idx_t i = 0; i < state.visited_paths.size(); i++) {
					if (!state.visited_paths[i]) {
						// TODO: Introduce overhead for reinitialization?
						reinit_cost_estimate += path_resistances[i] * init_tuple_count;
					}
				}

			// We visited all paths in the last round. Therefore, reset
			if (reinit_cost_estimate == 0) {
				std::fill(state.visited_paths.begin(), state.visited_paths.end(), false);
				state.visited_paths[min_resistance_path_idx] = true;

				for (idx_t i = 0; i < state.visited_paths.size(); i++) {
					reinit_cost_estimate += path_resistances[i] * init_tuple_count;
				}
			}

			double tuple_count_before_reinit = reinit_cost_estimate / (state.exploration_budget * min_resistance);
			state.window_size = tuple_count_before_reinit;
		}

		if (min_resistance <= state.RESISTANCE_TOLERANCE) {
		    state.window_offset = 0;
		    // Never reinitialize if we are within the tolerance
		    return min_resistance_path_idx;
		}

		if (state.window_offset >= state.window_size) {
			state.window_offset = 0;

			for (idx_t i = 0; i < state.visited_paths.size(); i++) {
				if (!state.visited_paths[i]) {
				    path_resistances[i] = 0;
				    state.init_phase_done = false;
				} else {
				    state.visited_paths[i] = false;
				}
			}
			state.init_phase_done = false;

			return DetermineNextPath();
		}

		return min_resistance_path_idx;
	}

	// Initialize the next best path
	auto &path_resistances = *state.path_resistances;
	for (idx_t i = 0; i < path_resistances.size(); i++) {
		if (path_resistances[i] == 0) {
			return i;
		}
	}

	// Everything initialized already
	state.init_phase_done = true;
	return DetermineNextPath();
}

idx_t AdaptiveReinitRoutingStrategy::DetermineNextTupleCount() const {
	auto &state = (AdaptiveReinitRoutingStrategyState &)*routing_state;

	if (state.init_phase_done) {
		if (state.window_offset < state.window_size) {
			state.num_cache_flushing_skips = (idx_t)std::round(state.window_size / (double)state.chunk_size);
			state.window_offset += state.window_size;
		} else {
			state.num_cache_flushing_skips = 0;
		}
		return state.chunk_size - state.chunk_offset;
	}

	state.num_cache_flushing_skips = 0;
	return std::min(init_tuple_count, state.chunk_size - state.chunk_offset);
}

idx_t ExponentialBackoffRoutingStrategy::DetermineNextPath() const {
	auto &state = (ExponentialBackoffRoutingStrategyState &)*routing_state;

	if (state.init_phase_done) {
		auto &path_resistances = *state.path_resistances;

		double current_min_resistance = path_resistances.front();
		idx_t current_min_resistance_path_idx = 0;

		for (idx_t i = 1; i < path_resistances.size(); i++) {
			if (path_resistances[i] < current_min_resistance) {
				current_min_resistance_path_idx = i;
				current_min_resistance = path_resistances[i];
			}
		}

		if (state.window_offset == 0) {
			if (state.window_size == 0) {
				state.window_size = 1;
			} else if (current_min_resistance_path_idx == state.min_resistance_path_idx ||
			           current_min_resistance * state.resistance_tolerance >=
			               path_resistances[state.min_resistance_path_idx]) {
				state.window_size = std::min(state.max_window_size, state.window_size * 2);
			} else {
				state.window_size = 1;
			}
		} else if (state.window_offset >= state.window_size) {
			state.window_offset = 0;
			state.init_phase_done = false;
			for (idx_t i = 0; i < path_resistances.size(); i++) {
				if (i != state.min_resistance_path_idx) {
					path_resistances[i] = 0;
				}
			}

			return DetermineNextPath();
		}

		state.min_resistance = current_min_resistance;
		state.min_resistance_path_idx = current_min_resistance_path_idx;
		return current_min_resistance_path_idx;
	}

	// Initialize the next best path
	auto &path_resistances = *state.path_resistances;
	for (idx_t i = 0; i < path_resistances.size(); i++) {
		if (path_resistances[i] == 0) {
			return i;
		}
	}

	// Everything initialized already
	state.init_phase_done = true;
	return DetermineNextPath();
}

idx_t ExponentialBackoffRoutingStrategy::DetermineNextTupleCount() const {
	auto &state = (ExponentialBackoffRoutingStrategyState &)*routing_state;

	if (state.init_phase_done) {
		state.num_cache_flushing_skips = state.window_size;
		state.window_offset += state.window_size;
		return state.chunk_size - state.chunk_offset;
	}

	state.num_cache_flushing_skips = 0;
	return std::min(init_tuple_count, state.chunk_size - state.chunk_offset);
}

void CalculateJoinPathWeights(const vector<double> &join_path_costs, vector<double> &path_weights,
                              double regret_budget) {
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

idx_t DynamicRoutingStrategy::DetermineNextPath() const {
	auto &state = (DynamicRoutingStrategyState &)*routing_state;

	if (state.init_phase_done) {
		idx_t max_remaining_tuples = state.remaining_tuples.front();
		idx_t max_remaining_tuples_path_idx = 0;
		for (idx_t i = 1; i < state.remaining_tuples.size(); i++) {
			if (state.remaining_tuples[i] > max_remaining_tuples) {
				max_remaining_tuples = state.remaining_tuples[i];
				max_remaining_tuples_path_idx = i;
			}
		}

		if (max_remaining_tuples > 0) {
			return max_remaining_tuples_path_idx;
		}

		// We routed all the tuples from our last weight calculations. Let's recalculate then
		std::fill(state.path_weights.begin(), state.path_weights.end(), 1);
		CalculateJoinPathWeights(*state.path_resistances, state.path_weights, state.regret_budget);

		idx_t input_tuples = state.chunk_size * multiplier - state.chunk_offset;
		idx_t remaining_tuples_sum = 0;
		for (idx_t i = 0; i < state.path_weights.size(); i++) {
			int remaining_tuples = state.remaining_tuples_diff[i] + std::round(state.path_weights[i] * input_tuples);

			if (remaining_tuples < 0) {
				// We do not allow negative remaining tuples
				state.remaining_tuples_diff[i] += state.remaining_tuples[i];
				state.remaining_tuples[i] = 0;
			} else {
				state.remaining_tuples[i] = remaining_tuples;
				state.remaining_tuples_diff[i] = 0;
			}
			remaining_tuples_sum += state.remaining_tuples[i];
		}

		idx_t remaining_tuples_sum_after_normalization = 0;
		// Normalize
		for (idx_t i = 0; i < state.remaining_tuples.size(); i++) {
			state.remaining_tuples[i] =
			    std::round(state.remaining_tuples[i] / (double)remaining_tuples_sum * input_tuples);
			if (state.remaining_tuples[i] < 64) { // TODO: instead set to init tuple count?
				state.remaining_tuples_diff[i] = state.remaining_tuples[i];
				state.remaining_tuples[i] = 0;
			}
			remaining_tuples_sum_after_normalization += state.remaining_tuples[i];
		}

		if (remaining_tuples_sum_after_normalization != input_tuples) {
			idx_t control_sum = 0;
			idx_t max_normalized = 0;
			idx_t max_normalized_idx = 0;

			for (idx_t i = 0; i < state.remaining_tuples.size(); i++) {
				if (state.remaining_tuples[i] > 0) {
					idx_t normalized = std::round(state.remaining_tuples[i] /
					                              (double)remaining_tuples_sum_after_normalization * input_tuples);
					state.remaining_tuples_diff[i] -= normalized - state.remaining_tuples[i];
					state.remaining_tuples[i] = normalized;
					control_sum += normalized;

					if (normalized > max_normalized) {
						max_normalized = normalized;
						max_normalized_idx = i;
					}
				}
			}
			if (control_sum != input_tuples) {
				state.remaining_tuples[max_normalized_idx] -= control_sum - (int)input_tuples;
				control_sum -= control_sum - (int)input_tuples;
			}
			D_ASSERT(control_sum == input_tuples);
		}
		return DetermineNextPath();
	}

	// Initialize the next best path
	auto &path_resistances = *state.path_resistances;
	for (idx_t i = 0; i < path_resistances.size(); i++) {
		if (path_resistances[i] == 0) {
			return i;
		}
	}

	// Everything initialized already
	state.init_phase_done = true;
	return DetermineNextPath();
}

idx_t DynamicRoutingStrategy::DetermineNextTupleCount() const {
	auto &state = (DynamicRoutingStrategyState &)*routing_state;
	state.num_cache_flushing_skips = 0;

	if (state.init_phase_done) {
		idx_t max_remaining_tuples = state.remaining_tuples.front();
		idx_t max_remaining_tuples_path_idx = 0;
		for (idx_t i = 1; i < state.remaining_tuples.size(); i++) {
			if (state.remaining_tuples[i] > max_remaining_tuples) {
				max_remaining_tuples = state.remaining_tuples[i];
				max_remaining_tuples_path_idx = i;
			}
		}

		if (max_remaining_tuples > 0) {
			idx_t remaining_input_tuples = state.chunk_size - state.chunk_offset;

			if (max_remaining_tuples > remaining_input_tuples) {
				state.num_cache_flushing_skips = (max_remaining_tuples - remaining_input_tuples) / state.chunk_size;
				state.remaining_tuples[max_remaining_tuples_path_idx] -=
				    state.num_cache_flushing_skips * state.chunk_size + remaining_input_tuples;
				return remaining_input_tuples;
			}

			state.remaining_tuples[max_remaining_tuples_path_idx] = 0;
			return max_remaining_tuples;
		}
	}

	return std::min(init_tuple_count, state.chunk_size - state.chunk_offset);
}

OperatorResultType AlternateRoutingStrategy::Route(DataChunk &input, DataChunk &chunk) const {
	routing_state->next_path_idx = routing_state->next_tuple_count == 0
	                                   ? 0
	                                   : ++routing_state->next_path_idx % routing_state->path_resistances->size();
	routing_state->next_tuple_count = input.size();
	chunk.Reference(input);

	if (routing_state->next_path_idx == routing_state->path_resistances->size() - 1) {
		return OperatorResultType::NEED_MORE_INPUT;
	} else {
		return OperatorResultType::HAVE_MORE_OUTPUT;
	}
}

idx_t DefaultPathRoutingStrategy::DetermineNextPath() const {
	routing_state->num_cache_flushing_skips = std::numeric_limits<idx_t>::max();
	return 0;
}

idx_t DefaultPathRoutingStrategy::DetermineNextTupleCount() const {
	return routing_state->chunk_size;
}

} // namespace duckdb
