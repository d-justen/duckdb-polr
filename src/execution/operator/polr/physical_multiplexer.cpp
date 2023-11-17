#include "duckdb/execution/operator/polr/physical_multiplexer.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/operator/polr/routing_strategy.hpp"

#include <algorithm>
#include <chrono>
#include <map>
#include <iostream>
#include <fstream>

namespace duckdb {

PhysicalMultiplexer::PhysicalMultiplexer(vector<LogicalType> types, idx_t estimated_cardinality, idx_t path_count_p,
                                         double regret_budget_p, MultiplexerRouting routing_p)
    : PhysicalOperator(PhysicalOperatorType::MULTIPLEXER, move(types), estimated_cardinality), path_count(path_count_p),
      regret_budget(regret_budget_p), routing(routing_p) {
}

class MultiplexerState : public OperatorState {
public:
	MultiplexerState(idx_t path_count, MultiplexerRouting routing, double regret_budget)
	    : path_resistances(path_count, 0), historic_resistances(path_count, 0),
	      input_tuple_count_per_path(path_count, 0) {
		switch (routing) {
		case MultiplexerRouting::ADAPTIVE_REINIT:
			routing_strategy = make_unique<AdaptiveReinitRoutingStrategy>(&path_resistances, regret_budget);
			break;
		case MultiplexerRouting::ALTERNATE:
			routing_strategy = make_unique<AlternateRoutingStrategy>(&path_resistances);
			break;
		case MultiplexerRouting::DYNAMIC:
			routing_strategy = make_unique<DynamicRoutingStrategy>(&path_resistances, regret_budget);
			break;
		case MultiplexerRouting::INIT_ONCE:
			routing_strategy = make_unique<InitOnceRoutingStrategy>(&path_resistances);
			break;
		case MultiplexerRouting::OPPORTUNISTIC:
			routing_strategy = make_unique<OpportunisticRoutingStrategy>(&path_resistances);
			break;
		case MultiplexerRouting::DEFAULT_PATH:
			routing_strategy = make_unique<DefaultPathRoutingStrategy>(&path_resistances);
			break;
		case MultiplexerRouting::BACKPRESSURE:
			routing_strategy = make_unique<DefaultPathRoutingStrategy>(&path_resistances);
			break;
		case MultiplexerRouting::EXPONENTIAL_BACKOFF:
			routing_strategy = make_unique<ExponentialBackoffRoutingStrategy>(&path_resistances, (idx_t)regret_budget);
			break;
		default:
			D_ASSERT(false); // TODO throw
		}
	}

	unique_ptr<RoutingStrategy> routing_strategy;
	vector<double> path_resistances;
	vector<double> historic_resistances;
	vector<idx_t> input_tuple_count_per_path;

	bool first_mpx_run = true;
	idx_t num_intermediates_current_path = 0;
	idx_t num_tuples_processed = 0;
	idx_t current_path_tuple_count = 0;
	idx_t current_path_idx = 0;

	vector<vector<idx_t>> intermediates_alternate_mode;
	vector<idx_t> intermediates_per_round;

	idx_t num_cache_flushing_skips = 0;

	bool use_time_resistance = false;
	std::chrono::time_point<std::chrono::system_clock> path_begin;
	std::chrono::time_point<std::chrono::system_clock> path_end;

public:
	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
	}
};

unique_ptr<OperatorState> PhysicalMultiplexer::GetOperatorState(ExecutionContext &context) const {
	auto state = make_unique<MultiplexerState>(path_count, routing, regret_budget);
	if (context.client.config.time_resistance) {
		state->use_time_resistance = true;
	}

	return move(state);
}

idx_t &PhysicalMultiplexer::GetNumCacheFlushingSkips(OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;
	return state.num_cache_flushing_skips;
}

OperatorResultType PhysicalMultiplexer::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;

	if (!state.first_mpx_run) {
		// TODO: Always finalize from outside
		state.path_end = std::chrono::system_clock::now();
		FinalizePathRun(state, context.client.config.log_tuples_routed);
	} else {
		state.first_mpx_run = false;
		if (routing == MultiplexerRouting::ALTERNATE) {
			state.intermediates_alternate_mode = vector<vector<idx_t>>(state.path_resistances.size());
		}
	}

	auto result = state.routing_strategy->Route(input, chunk);
	state.current_path_tuple_count = chunk.size();
	state.current_path_idx = state.routing_strategy->routing_state->next_path_idx;
	state.num_cache_flushing_skips = state.routing_strategy->routing_state->num_cache_flushing_skips;
	state.path_begin = std::chrono::system_clock::now();
	return result;
}

string PhysicalMultiplexer::ParamsToString() const {
	return "";
}

void PhysicalMultiplexer::IncreaseInputTupleCount(OperatorState &state_p, idx_t tuple_count) const {
	auto &state = (MultiplexerState &)state_p;
	state.current_path_tuple_count += tuple_count;
}

void PhysicalMultiplexer::FinalizePathRun(OperatorState &state_p, bool log_tuples_routed) const {
	auto &state = (MultiplexerState &)state_p;

	state.input_tuple_count_per_path[state.current_path_idx] += state.current_path_tuple_count;
	state.num_tuples_processed += state.current_path_tuple_count;

	if (log_tuples_routed) {
		state.intermediates_per_round.push_back(state.num_intermediates_current_path);
	}

	if (!state.intermediates_alternate_mode.empty()) {
		state.intermediates_alternate_mode[state.current_path_idx].push_back(state.num_intermediates_current_path);
		state.num_intermediates_current_path = 0;
		return;
	}

	double path_resistance;
	if (state.use_time_resistance) {
		path_resistance =
		    (state.path_end - state.path_begin).count() / static_cast<double>(state.current_path_tuple_count);
	} else {
		// If there are no intermediates, we want to add a very small number so that we don't have to process 0s in the
		// weight calculation
		double constant_overhead = 0.5;
		path_resistance = state.num_intermediates_current_path / static_cast<double>(state.current_path_tuple_count) +
		                  constant_overhead;
	}

	if (state.historic_resistances[state.current_path_idx] != 0) {
		if (state.current_path_tuple_count > 1024 && routing == MultiplexerRouting::ADAPTIVE_REINIT &&
		    state.use_time_resistance) {
			path_resistance = state.historic_resistances[state.current_path_idx];
		} else {
			path_resistance = state.historic_resistances[state.current_path_idx] * SMOOTHING_FACTOR +
			                  (1 - SMOOTHING_FACTOR) * path_resistance;
		}
	}

	state.path_resistances[state.current_path_idx] = path_resistance;
	state.historic_resistances[state.current_path_idx] = path_resistance;

	state.num_intermediates_current_path = 0;
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
	std::stringstream log;

	if (!state.intermediates_alternate_mode.empty()) {
		for (idx_t i = 0; i < state.intermediates_alternate_mode.size(); i++) {
			log << "path_" << i << ",";
		}
		log << "\n";

		for (idx_t i = 0; i < state.intermediates_alternate_mode.front().size(); i++) {
			for (idx_t j = 0; j < state.intermediates_alternate_mode.size(); j++) {
				log << state.intermediates_alternate_mode[j][i] << ",";
			}
			log << "\n";
		}
	} else {
		log << "intermediates\n";

		for (idx_t i = 0; i < state.intermediates_per_round.size(); i++) {
			log << state.intermediates_per_round[i] << "\n";
		}
	}

	file << log.str();
}

bool PhysicalMultiplexer::WasExecuted(OperatorState &state_p) const {
	auto &state = (MultiplexerState &)state_p;
	for (auto input_tuple_count : state.input_tuple_count_per_path) {
		if (input_tuple_count > 0) {
			return true;
		}
	}
	return false;
}

} // namespace duckdb
