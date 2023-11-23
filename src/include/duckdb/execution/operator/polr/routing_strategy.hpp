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
	idx_t num_cache_flushing_skips = 0;
};

class RoutingStrategy {
public:
	RoutingStrategy(vector<double> *path_resistances,
	                         idx_t init_tuple_count_p,
	                         std::unique_ptr<RoutingStrategyState> routing_state_p = nullptr) :
	init_tuple_count(init_tuple_count_p) {
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
	const idx_t init_tuple_count;

protected:
	OperatorResultType SelectTuples(DataChunk &input, DataChunk &chunk) const;

	virtual idx_t DetermineNextPath() const = 0;
	virtual idx_t DetermineNextTupleCount() const = 0;
};

class OpportunisticRoutingStrategy : public RoutingStrategy {
public:
	explicit OpportunisticRoutingStrategy(vector<double> *path_resistances, idx_t init_tuple_count_p) : RoutingStrategy(path_resistances, init_tuple_count_p) {
	}

protected:
	idx_t DetermineNextPath() const override;
	idx_t DetermineNextTupleCount() const override;
};

class InitOnceRoutingStrategyState : public RoutingStrategyState {
public:
	explicit InitOnceRoutingStrategyState(vector<double> *path_resistances) : RoutingStrategyState(path_resistances) {
	}

	idx_t best_path_after_init = 0;
	idx_t num_paths_initialized = 0;
	bool init_phase_done = false;
};

class InitOnceRoutingStrategy : public RoutingStrategy {
public:
	explicit InitOnceRoutingStrategy(vector<double> *path_resistances, idx_t init_tuple_count_p)
	    : RoutingStrategy(nullptr, init_tuple_count_p, make_unique<InitOnceRoutingStrategyState>(path_resistances)) {
	}

protected:
	idx_t DetermineNextPath() const override;
	idx_t DetermineNextTupleCount() const override;
};

class AdaptiveReinitRoutingStrategyState : public RoutingStrategyState {
public:
	AdaptiveReinitRoutingStrategyState(vector<double> *path_resistances, double exploration_budget_p)
	    : RoutingStrategyState(path_resistances), exploration_budget(exploration_budget_p) {
		visited_paths.resize(path_resistances->size(), false);
	}

	const double exploration_budget;
	bool init_phase_done = false;

	idx_t window_offset = 0;
	idx_t window_size = 0;
	vector<bool> visited_paths;
	double RESISTANCE_TOLERANCE = 0.525;
};

// TODO: Can inherit from InitOnce: Next tuple count is the same
class AdaptiveReinitRoutingStrategy : public RoutingStrategy {
public:
	explicit AdaptiveReinitRoutingStrategy(vector<double> *path_resistances, double exploration_budget, idx_t init_tuple_count_p)
	    : RoutingStrategy(path_resistances, init_tuple_count_p,
	                      make_unique<AdaptiveReinitRoutingStrategyState>(path_resistances, exploration_budget)) {
	}

protected:
	idx_t DetermineNextPath() const override;
	idx_t DetermineNextTupleCount() const override;
};

class ExponentialBackoffRoutingStrategyState : public RoutingStrategyState {
public:
	ExponentialBackoffRoutingStrategyState(vector<double> *path_resistances, idx_t max_window_size_p)
	    : RoutingStrategyState(path_resistances), max_window_size(max_window_size_p) {
	}

	const idx_t max_window_size;
	const idx_t init_tuple_count = 64;
	const double resistance_tolerance = 1.1;
	idx_t min_resistance_path_idx = -1;
	double min_resistance = std::numeric_limits<double>::max();
	bool init_phase_done = false;

	idx_t window_offset = 0;
	idx_t window_size = 0;
};

class ExponentialBackoffRoutingStrategy : public RoutingStrategy {
public:
	explicit ExponentialBackoffRoutingStrategy(vector<double> *path_resistances, idx_t max_window_size_p, idx_t init_tuple_count_p)
	    : RoutingStrategy(path_resistances, init_tuple_count_p,
	                      make_unique<ExponentialBackoffRoutingStrategyState>(path_resistances, max_window_size_p)) {
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

	const double regret_budget;
	bool init_phase_done = false;

	vector<idx_t> remaining_tuples;
	vector<int64_t> remaining_tuples_diff;
	vector<double> path_weights;
};

class DynamicRoutingStrategy : public RoutingStrategy {
public:
	DynamicRoutingStrategy(vector<double> *path_resistances, double exploration_budget, idx_t init_tuple_count_p, idx_t multiplier_p)
	    : multiplier(multiplier_p), RoutingStrategy(path_resistances, init_tuple_count_p,
	                      make_unique<DynamicRoutingStrategyState>(path_resistances, exploration_budget)) {
	}

protected:
	idx_t DetermineNextPath() const override;
	idx_t DetermineNextTupleCount() const override;

	const idx_t multiplier;
};

class AlternateRoutingStrategy : public RoutingStrategy {
public:
	explicit AlternateRoutingStrategy(vector<double> *path_resistances) : RoutingStrategy(path_resistances, 0) {
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
	explicit DefaultPathRoutingStrategy(vector<double> *path_resistances) : RoutingStrategy(path_resistances, 0) {
	}

protected:
	idx_t DetermineNextPath() const override;
	idx_t DetermineNextTupleCount() const override;
};

} // namespace duckdb
