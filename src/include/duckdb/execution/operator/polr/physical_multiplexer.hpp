//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/polr/physical_multiplexer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalMultiplexer : public PhysicalOperator {
public:
	PhysicalMultiplexer(vector<LogicalType> types, idx_t estimated_cardinality, idx_t path_count_p,
	                    double regret_budget_p = 0.2);

	idx_t path_count;
	double regret_budget;

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

private:
	void CalculateJoinPathWeights(const vector<double> &join_path_costs, vector<double> &path_weights) const;
};

} // namespace duckdb
