//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/polr/physical_multiplexer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

class PhysicalMultiplexer : public PhysicalOperator {
public:
	PhysicalMultiplexer(vector<LogicalType> types, idx_t estimated_cardinality, idx_t path_count_p,
	                    double regret_budget_p, MultiplexerRouting routing);

	idx_t path_count;
	double regret_budget;
	MultiplexerRouting routing;
	const double SMOOTHING_FACTOR = 0.5;

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	void FinalizePathRun(OperatorState &state_p, bool log_tuples_routed) const;
	void AddNumIntermediates(OperatorState &state_p, idx_t count) const;
	idx_t GetCurrentPathIndex(OperatorState &state_p) const;

	bool ParallelOperator() const override {
		return true;
	}
	bool RequiresCache() const override {
		return false;
	}

	string ParamsToString() const override;
	void PrintStatistics(OperatorState &state) const;
	void WriteLogToFile(OperatorState &state, std::ofstream &file) const;
	bool WasExecuted(OperatorState &state_p) const;
	idx_t &GetNumCacheFlushingSkips(OperatorState &state_p) const;
	void IncreaseInputTupleCount(OperatorState &state_p, idx_t tuple_count) const;
};

} // namespace duckdb
