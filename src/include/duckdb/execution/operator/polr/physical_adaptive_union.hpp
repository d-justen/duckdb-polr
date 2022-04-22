//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/polr/physical_adaptive_union.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalAdaptiveUnion : public PhysicalOperator {
public:
	PhysicalAdaptiveUnion(vector<LogicalType> types, idx_t estimated_cardinality);

public:
	unique_ptr<OperatorState> GetOperatorState(ClientContext &context) const override;

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return false; // TODO: Switch to parallel op once we have a stable prototype
	}
	bool RequiresCache() const override {
		return false; // TODO: Maybe we do want this at some point
	}

	string ParamsToString() const override;
};

} // namespace duckdb
