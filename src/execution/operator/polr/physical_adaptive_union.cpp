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
                                                  GlobalOperatorState &gstate_p,OperatorState &state_p) const {
	context.thread.update_path_weight();
	chunk.Append(input, true);

	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalAdaptiveUnion::ParamsToString() const {
	return "";
}

} // namespace duckdb
