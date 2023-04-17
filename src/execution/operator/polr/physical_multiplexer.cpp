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
                                         double regret_budget_p, MultiplexerRouting routing)
    : PhysicalOperator(PhysicalOperatorType::MULTIPLEXER, move(types), estimated_cardinality), path_count(path_count_p),
      regret_budget(regret_budget_p), routing_strategy(routing) {
	switch (routing) {
	case MultiplexerRouting::ADAPTIVE_REINIT: {
		router_function = [=](ExecutionContext &e, DataChunk &i, DataChunk &c, OperatorState &s) {
			return RouteAdaptiveReinit(e, i, c, s);
		};
		break;
	}
	case MultiplexerRouting::ALTERNATE: {
		router_function = [=](ExecutionContext &e, DataChunk &i, DataChunk &c, OperatorState &s) {
			return RouteAlternate(e, i, c, s);
		};
		break;
	}
	case MultiplexerRouting::DYNAMIC: {
		router_function = [=](ExecutionContext &e, DataChunk &i, DataChunk &c, OperatorState &s) {
			return RouteDynamic(e, i, c, s);
		};
		break;
	}
	case MultiplexerRouting::INIT_ONCE: {
		router_function = [=](ExecutionContext &e, DataChunk &i, DataChunk &c, OperatorState &s) {
			return RouteInitializeOnce(e, i, c, s);
		};
		break;
	}
	case MultiplexerRouting::OPPORTUNISTIC: {
		router_function = [=](ExecutionContext &e, DataChunk &i, DataChunk &c, OperatorState &s) {
			return RouteOpportunistic(e, i, c, s);
		};
		break;
	}
	}
}

class MultiplexerState : public OperatorState {
public:
	MultiplexerState(idx_t path_count) {
		intermediates_per_input_tuple.resize(path_count, 0);
		input_tuple_count_per_path.resize(path_count, 0);
		path_resistances.resize(path_count, 0);
		visited_paths.resize(path_count, false);
		remaining_tuples.resize(path_count, 0);
		remaining_tuples_diff.resize(path_count, 0);
		path_weights.resize(path_count, 1);
		init_tuple_count = 128 / path_count;
		sel.Initialize();
	}

	bool first_mpx_run = true;
	idx_t num_paths_initialized = 0;

	// To be set before running the path
	idx_t chunk_offset = 0;
	idx_t current_path_idx = 0;
	idx_t current_path_tuple_count = 0;
	idx_t current_path_remaining_tuples = 0;
	vector<idx_t> remaining_tuples;
	vector<int> remaining_tuples_diff;

	// To be set during path run
	idx_t num_intermediates_current_path = 0;

	// To be set after running the path
	vector<double> intermediates_per_input_tuple;
	idx_t num_tuples_processed = 0;
	vector<idx_t> input_tuple_count_per_path;
	std::unique_ptr<idx_t> best_path_after_init;

	idx_t init_tuple_count = 8;
	std::stringstream log;
	vector<vector<idx_t>> intermediates_alternate_mode;

	idx_t window_size = 0;
	idx_t window_idx = 0;
	vector<double> path_resistances;
	vector<bool> visited_paths;
	vector<double> path_weights;
	const double smoothing_factor = 0.5;

	SelectionVector sel;

public:
	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
	}
};

unique_ptr<OperatorState> PhysicalMultiplexer::GetOperatorState(ExecutionContext &context) const {
	return make_unique<MultiplexerState>(path_count);
}

OperatorResultType PhysicalMultiplexer::InitPath(DataChunk &input, DataChunk &chunk, OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;

	D_ASSERT(state.num_paths_initialized < path_count);
	idx_t next_path_idx = -1;
	for (idx_t i = 0; i < state.path_resistances.size(); i++) {
		if (state.path_resistances[i] == 0) {
			next_path_idx = i;
			break;
		}
	}
	D_ASSERT(next_path_idx < path_count);
	D_ASSERT(input.size() > state.chunk_offset);
	idx_t remaining_input_tuples = input.size() - state.chunk_offset;
	idx_t tuple_count =
	    remaining_input_tuples > state.init_tuple_count ? state.init_tuple_count : remaining_input_tuples;

	auto *sel_vector = state.sel.data();

	for (idx_t i = 0; i < tuple_count; i++) {
		sel_vector[i] = state.chunk_offset + i;
	}

	chunk.Slice(input, state.sel, tuple_count);

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

OperatorResultType PhysicalMultiplexer::RouteTuples(DataChunk &input, DataChunk &chunk, OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;

	if (state.current_path_tuple_count == input.size()) {
		D_ASSERT(state.chunk_offset == 0);
		chunk.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	D_ASSERT(state.chunk_offset + state.current_path_tuple_count <= input.size());

	auto *sel_vector = state.sel.data();

	for (idx_t i = 0; i < state.current_path_tuple_count; i++) {
		sel_vector[i] = state.chunk_offset + i;
	}

	chunk.Slice(input, state.sel, state.current_path_tuple_count);

	if (state.chunk_offset + state.current_path_tuple_count == input.size()) {
		state.chunk_offset = 0;
		return OperatorResultType::NEED_MORE_INPUT;
	}

	state.chunk_offset += state.current_path_tuple_count;
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

OperatorResultType PhysicalMultiplexer::RouteInitializeOnce(ExecutionContext &context, DataChunk &input,
                                                            DataChunk &chunk, OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;

	if (state.num_paths_initialized < path_count) {
		return InitPath(input, chunk, state);
	}

	if (!state.best_path_after_init) {
		double min_resistance = state.path_resistances.front();
		idx_t path_idx = 0;

		for (idx_t i = 1; i < state.path_resistances.size(); i++) {
			if (state.path_resistances[i] < min_resistance) {
				min_resistance = state.path_resistances[i];
				path_idx = i;
			}
		}
		state.best_path_after_init = make_unique<idx_t>(path_idx);
		state.current_path_idx = path_idx;
	}

	D_ASSERT(input.size() > state.chunk_offset);
	D_ASSERT(((int64_t)input.size() - state.chunk_offset) > 0);
	idx_t remaining_input_tuples = input.size() - state.chunk_offset;
	D_ASSERT(remaining_input_tuples <= STANDARD_VECTOR_SIZE);
	state.current_path_tuple_count = remaining_input_tuples;

	return RouteTuples(input, chunk, state);
}

OperatorResultType PhysicalMultiplexer::RouteAdaptiveReinit(ExecutionContext &context, DataChunk &input,
                                                            DataChunk &chunk, OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;

	if (state.num_paths_initialized < path_count) {
		return InitPath(input, chunk, state);
	}

	double min_resistance = state.path_resistances.front();
	idx_t min_resistance_path_idx = 0;

	for (idx_t i = 1; i < state.path_resistances.size(); i++) {
		if (state.path_resistances[i] < min_resistance) {
			min_resistance_path_idx = i;
			min_resistance = state.path_resistances[i];
		}
	}

	D_ASSERT(input.size() > state.chunk_offset);
	idx_t remaining_input_tuples = input.size() - state.chunk_offset;
	state.current_path_tuple_count = remaining_input_tuples;
	state.current_path_idx = min_resistance_path_idx;

	// TODO declare constant
	if (min_resistance <= 0.2) {
		state.visited_paths[state.current_path_idx] = true;
		return RouteTuples(input, chunk, state);
	}

	// Either first run after initialization or path switch
	if (state.window_idx == 0 || !state.visited_paths[state.current_path_idx]) {
		// After (re-) initialization, we are about to send the first chunk to the path of least resistance
		// Find the next path and estimate the cost for the next initialization
		state.current_path_idx = min_resistance_path_idx;
		state.visited_paths[state.current_path_idx] = true;

		double reinit_cost_estimate = 0;
		for (idx_t i = 0; i < state.visited_paths.size(); i++) {
			if (!state.visited_paths[i]) {
				reinit_cost_estimate += state.path_resistances[i] * state.init_tuple_count;
			}
		}

		double tuple_count_before_reinit = reinit_cost_estimate / (regret_budget * min_resistance);
		state.window_size = tuple_count_before_reinit;
	}

	if (state.window_idx > state.window_size) {
		state.window_idx = 0;

		for (idx_t i = 0; i < state.visited_paths.size(); i++) {
			if (!state.visited_paths[i]) {
				state.path_resistances[i] = 0;
				state.num_paths_initialized--;
			} else {
				state.visited_paths[i] = false;
			}
		}

		return RouteAdaptiveReinit(context, input, chunk, state);
	}

	state.window_idx += state.current_path_tuple_count;
	state.current_path_idx = min_resistance_path_idx;
	return RouteTuples(input, chunk, state);
}

OperatorResultType PhysicalMultiplexer::RouteDynamic(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                     OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;

	if (state.num_paths_initialized < path_count) {
		return InitPath(input, chunk, state);
	}

	idx_t max_remaining_tuples = state.remaining_tuples.front();
	idx_t max_remaining_tuples_path_idx = 0;
	for (idx_t i = 1; i < state.remaining_tuples.size(); i++) {
		if (state.remaining_tuples[i] > max_remaining_tuples) {
			max_remaining_tuples = state.remaining_tuples[i];
			max_remaining_tuples_path_idx = i;
		}
	}

	if (max_remaining_tuples > 0) {
		state.current_path_idx = max_remaining_tuples_path_idx;
		idx_t remaining_input_tuples = input.size() - state.chunk_offset;

		if (max_remaining_tuples > remaining_input_tuples) {
			state.current_path_tuple_count = remaining_input_tuples;
			state.remaining_tuples[max_remaining_tuples_path_idx] -= remaining_input_tuples;
		} else {
			state.current_path_tuple_count = max_remaining_tuples;
			state.remaining_tuples[max_remaining_tuples_path_idx] = 0;
		}

		return RouteTuples(input, chunk, state);
	}

	// We routed all the tuples from our last weight calculations. Let's recalculate then
	std::fill(state.path_weights.begin(), state.path_weights.end(), 1);
	CalculateJoinPathWeights(state.path_resistances, state.path_weights);

	idx_t remaining_tuples_sum = 0;
	for (idx_t i = 0; i < state.path_weights.size(); i++) {
		int remaining_tuples = state.remaining_tuples_diff[i] + std::round(state.path_weights[i] * input.size());

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
		state.remaining_tuples[i] = std::round(state.remaining_tuples[i] / (double)remaining_tuples_sum * input.size());
		if (state.remaining_tuples[i] < 64) {
			state.remaining_tuples_diff[i] = state.remaining_tuples[i];
			state.remaining_tuples[i] = 0;
		}
		remaining_tuples_sum_after_normalization += state.remaining_tuples[i];
	}

	if (remaining_tuples_sum_after_normalization != input.size()) {

		idx_t control_sum = 0;
		idx_t max_normalized = 0;
		idx_t max_normalized_idx = 0;

		for (idx_t i = 0; i < state.remaining_tuples.size(); i++) {
			if (state.remaining_tuples[i] > 0) {
				idx_t normalized = std::round(state.remaining_tuples[i] /
				                              (double)remaining_tuples_sum_after_normalization * input.size());
				state.remaining_tuples_diff[i] -= normalized - state.remaining_tuples[i];
				state.remaining_tuples[i] = normalized;
				control_sum += normalized;

				if (normalized > max_normalized) {
					max_normalized = normalized;
					max_normalized_idx = i;
				}
			}
		}
		if (control_sum != input.size()) {
			state.remaining_tuples[max_normalized_idx] -= control_sum - (int)input.size();
			control_sum -= control_sum - (int)input.size();
		}
		D_ASSERT(control_sum == input.size());
	}

	return RouteDynamic(context, input, chunk, state);
}

OperatorResultType PhysicalMultiplexer::RouteAlternate(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                       OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;

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

OperatorResultType PhysicalMultiplexer::RouteOpportunistic(ExecutionContext &context, DataChunk &input,
                                                           DataChunk &chunk, OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;

	double min_resistance = std::numeric_limits<double>::max();
	for (idx_t i = 0; i < state.path_resistances.size(); i++) {
		if (state.path_resistances[i] < min_resistance) {
			min_resistance = state.path_resistances[i];
			state.current_path_idx = i;
		}
	}

	state.current_path_tuple_count = input.size();
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorResultType PhysicalMultiplexer::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;

	if (!state.first_mpx_run) {
		FinalizePathRun(state, context.client.config.log_tuples_routed);
		state.num_intermediates_current_path = 0;
	} else {
		state.first_mpx_run = false;
		if (routing_strategy == MultiplexerRouting::ALTERNATE) {
			state.intermediates_alternate_mode = vector<vector<idx_t>>(state.intermediates_per_input_tuple.size());
		}
	}

	return router_function(context, input, chunk, state);
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

void PhysicalMultiplexer::FinalizePathRun(OperatorState &state_p, bool log_tuples_routed) const {
	auto &state = (MultiplexerState &)state_p;

	state.input_tuple_count_per_path[state.current_path_idx] += state.current_path_tuple_count;
	state.num_tuples_processed += state.current_path_tuple_count;

	if (!state.intermediates_alternate_mode.empty()) {
		state.intermediates_alternate_mode[state.current_path_idx].push_back(state.num_intermediates_current_path);
		state.num_tuples_processed += state.current_path_tuple_count;

		return;
	}

	// If there are no intermediates, we want to add a very small number so that we don't have to process 0s in the
	// weight calculation
	double constant_overhead = 0.1;
	double path_resistance =
	    state.num_intermediates_current_path / static_cast<double>(state.current_path_tuple_count) + constant_overhead;

	if (state.num_paths_initialized < path_count) {
		// We found ourselves in a (re-) initialization phase
		state.path_resistances[state.current_path_idx] = path_resistance;
		state.num_paths_initialized++;
	} else {
		// Rolling average
		path_resistance = state.path_resistances[state.current_path_idx] * state.smoothing_factor +
		                  (1 - state.smoothing_factor) * path_resistance;
		state.path_resistances[state.current_path_idx] = path_resistance;
	}

	if (log_tuples_routed) {
		if (state.num_tuples_processed - state.current_path_tuple_count == 0) {
			// First run!
			for (idx_t i = 0; i < state.path_resistances.size(); i++) {
				state.log << "resistance_" << i << ",";
				state.log << "sent_" << i << ",";
			}

			state.log << "\n";
		}

		for (idx_t i = 0; i < state.path_resistances.size(); i++) {
			state.log << state.path_resistances[i] << ",";
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
