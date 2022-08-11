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
	PhysicalMultiplexer(vector<LogicalType> types, idx_t estimated_cardinality, idx_t path_count_p);

	idx_t path_count;

public:
	unique_ptr<OperatorState> GetOperatorState(ClientContext &context) const override;

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	void FinalizePathRun(OperatorState &state_p, idx_t num_intermediates) const;
	idx_t GetCurrentPathIndex(OperatorState &state_p) const;

	bool ParallelOperator() const override {
		return true;
	}
	bool RequiresCache() const override {
		return false;
	}

	string ParamsToString() const override;
	void PrintStatistics(OperatorState &state) const;
};

} // namespace duckdb
