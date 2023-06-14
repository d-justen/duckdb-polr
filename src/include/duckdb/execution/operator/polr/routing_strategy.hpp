//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/polr/physical_multiplexer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "physical_multiplexer.hpp"

namespace duckdb {

class RoutingStrategyState {
public:
	explicit RoutingStrategyState(vector<double> *path_resistances_p) : path_resistances(path_resistances_p) {
		sel.Initialize();
	}

	vector<double> *path_resistances;
	idx_t chunk_size = 0;

	idx_t next_path_idx = 0;
	idx_t next_tuple_count = 0;

	idx_t chunk_offset = 0;
	SelectionVector sel;
};

class RoutingStrategy {
public:
	explicit RoutingStrategy(vector<double> *path_resistances,
	                         std::unique_ptr<RoutingStrategyState> routing_state_p = nullptr) {
		if (!routing_state_p) {
			routing_state = make_unique<RoutingStrategyState>(path_resistances);
		} else {
			routing_state = move(routing_state_p);
		}
	}

	virtual ~RoutingStrategy() {
	}

	virtual OperatorResultType Route(DataChunk &input, DataChunk &chunk) const {
		routing_state->chunk_size = input.size();
		routing_state->next_path_idx = DetermineNextPath();
		routing_state->next_tuple_count = DetermineNextTupleCount();

		return SelectTuples(input, chunk);
	}

	std::unique_ptr<RoutingStrategyState> routing_state;

protected:
	OperatorResultType SelectTuples(DataChunk &input, DataChunk &chunk) const;

	virtual idx_t DetermineNextPath() const = 0;
	virtual idx_t DetermineNextTupleCount() const = 0;
};

class OpportunisticRoutingStrategy : public RoutingStrategy {
public:
	explicit OpportunisticRoutingStrategy(vector<double> *path_resistances) : RoutingStrategy(path_resistances) {
	}

protected:
	idx_t DetermineNextPath() const override;
	idx_t DetermineNextTupleCount() const override;
};

class InitOnceRoutingStrategyState : public RoutingStrategyState {
public:
	explicit InitOnceRoutingStrategyState(vector<double> *path_resistances) : RoutingStrategyState(path_resistances) {
	}

	const idx_t INIT_TUPLE_COUNT = 128;
	idx_t best_path_after_init = 0;
	idx_t num_paths_initialized = 0;
	bool init_phase_done = false;
};

class InitOnceRoutingStrategy : public RoutingStrategy {
public:
	explicit InitOnceRoutingStrategy(vector<double> *path_resistances)
	    : RoutingStrategy(nullptr, make_unique<InitOnceRoutingStrategyState>(path_resistances)) {
	}

protected:
	idx_t DetermineNextPath() const override;
	idx_t DetermineNextTupleCount() const override;
};

class AdaptiveReinitRoutingStrategyState : public RoutingStrategyState {
public:
	AdaptiveReinitRoutingStrategyState(vector<double> *path_resistances, double exploration_budget_p)
	    : RoutingStrategyState(path_resistances), init_tuple_count(128 / path_resistances->size()),
	      exploration_budget(exploration_budget_p) {
		visited_paths.resize(path_resistances->size(), false);
	}

	const idx_t init_tuple_count;
	const double exploration_budget;
	bool init_phase_done = false;

	idx_t window_offset = 0;
	idx_t window_size = 0;
	vector<bool> visited_paths;
	double RESISTANCE_TOLERANCE = 0.125;
};

// TODO: Can inherit from InitOnce: Next tuple count is the same
class AdaptiveReinitRoutingStrategy : public RoutingStrategy {
public:
	explicit AdaptiveReinitRoutingStrategy(vector<double> *path_resistances, double exploration_budget)
	    : RoutingStrategy(path_resistances,
	                      make_unique<AdaptiveReinitRoutingStrategyState>(path_resistances, exploration_budget)) {
	}

protected:
	idx_t DetermineNextPath() const override;
	idx_t DetermineNextTupleCount() const override;
};

class DynamicRoutingStrategyState : public RoutingStrategyState {
public:
	DynamicRoutingStrategyState(vector<double> *path_resistances, double regret_budget_p)
	    : RoutingStrategyState(path_resistances), regret_budget(regret_budget_p) {
		remaining_tuples.resize(path_resistances->size(), 0);
		remaining_tuples_diff.resize(path_resistances->size(), 0);
		path_weights.resize(path_resistances->size(), 0);
	}

	const idx_t init_tuple_count = 16;
	const double regret_budget;
	bool init_phase_done = false;

	vector<idx_t> remaining_tuples;
	vector<int64_t> remaining_tuples_diff;
	vector<double> path_weights;
};

class DynamicRoutingStrategy : public RoutingStrategy {
public:
	DynamicRoutingStrategy(vector<double> *path_resistances, double exploration_budget)
	    : RoutingStrategy(path_resistances,
	                      make_unique<DynamicRoutingStrategyState>(path_resistances, exploration_budget)) {
	}

protected:
	idx_t DetermineNextPath() const override;
	idx_t DetermineNextTupleCount() const override;
};

class AlternateRoutingStrategy : public RoutingStrategy {
public:
	explicit AlternateRoutingStrategy(vector<double> *path_resistances) : RoutingStrategy(path_resistances) {
	}
	OperatorResultType Route(DataChunk &input, DataChunk &chunk) const override;

protected:
	idx_t DetermineNextPath() const override {
		return -1;
	}
	idx_t DetermineNextTupleCount() const override {
		return -1;
	}
};

class DefaultPathRoutingStrategy : public RoutingStrategy {
public:
	explicit DefaultPathRoutingStrategy(vector<double> *path_resistances) : RoutingStrategy(path_resistances) {
	}

protected:
	idx_t DetermineNextPath() const override;
	idx_t DetermineNextTupleCount() const override;
};

} // namespace duckdb