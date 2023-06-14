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
	PhysicalAdaptiveUnion(vector<LogicalType> types, idx_t num_columns_from_left_p,
	                      vector<idx_t> num_columns_per_join_p, idx_t estimated_cardinality);

public:
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	unique_ptr<OperatorState> GetOperatorStateWithStaticJoinOrder(ExecutionContext &context,
	                                                              vector<idx_t> *input_join_order) const;

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;

	bool ParallelOperator() const override {
		return true;
	}
	bool RequiresCache() const override {
		return false; // TODO: Maybe we do want this at some point
	}

	string ParamsToString() const override;

public:
	const idx_t num_columns_from_left;
	const vector<idx_t> num_columns_per_join;
};

} // namespace duckdb
