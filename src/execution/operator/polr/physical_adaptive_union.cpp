#include "duckdb/execution/operator/polr/physical_adaptive_union.hpp"

#include "duckdb/parallel/thread_context.hpp"

#include <algorithm>
#include <chrono>

namespace duckdb {

// TODO: Parametrize AdaptiveUnion with some Multiplexer hook so that it can report back the duration per path
PhysicalAdaptiveUnion::PhysicalAdaptiveUnion(vector<LogicalType> types, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::MULTIPLEXER, move(types), estimated_cardinality) {
}

class AdaptiveUnionState : public OperatorState {

public:
	void Finalize(PhysicalOperator *op, ExecutionContext &context) override {
	}
};

unique_ptr<OperatorState> PhysicalAdaptiveUnion::GetOperatorState(ClientContext &context) const {
	return make_unique<AdaptiveUnionState>();
}

OperatorResultType PhysicalAdaptiveUnion::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                  GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	// TODO: Re-order vectors to reflect target column order
	idx_t num_joins = 2;
	idx_t columns_to_reorder = input.ColumnCount() - num_joins;
	// Somehow we need the join order here:
	// If it is [1,0] we assign each column (ColumnCount() - columns_to_reorder)..ColumnCount() its source join
	// Column 0: From left side
	// Column 1: From left side
	// Column 2: Join 1
	// Column 3: Join 0
	// => 1 -> C2, 0 -> C3
	// Order ascending

	chunk.Append(input, true);

	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalAdaptiveUnion::ParamsToString() const {
	return "";
}

} // namespace duckdb
