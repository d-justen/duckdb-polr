#include "duckdb/execution/operator/polr/physical_adaptive_union.hpp"

#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>
#include <chrono>
#include <map>

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
	D_ASSERT(context.thread.current_join_path);

	idx_t num_joins = context.thread.current_join_path->size();
	vector<idx_t>& current_join_path = *context.thread.current_join_path;
	idx_t num_columns_from_left = input.ColumnCount() - num_joins; // TODO: Is this only valid for one join condition?

	idx_t new_chunk_size = chunk.size() + input.size();
	if (new_chunk_size > chunk.capacity) {
		for (idx_t i = 0; i < chunk.data.size(); i++) {
			chunk.data[i].Resize(chunk.size(), new_chunk_size);
		}
		chunk.capacity = new_chunk_size;
	}

	// TODO: This mapping only works under the condition that the join order [0,1,..,n] produces the correct column order
	vector<idx_t> column_idxs(num_joins);
	for (idx_t i = 0; i < num_joins; i++) {
		column_idxs[current_join_path[i]] = num_columns_from_left + i;
	}

	for (idx_t i = 0; i < num_columns_from_left; i++) {
		VectorOperations::Copy(input.data[i], chunk.data[i], input.size(), 0, chunk.size());
	}

	for (idx_t i = 0; i < column_idxs.size(); i++) {
		VectorOperations::Copy(input.data[num_columns_from_left + i], chunk.data[column_idxs[i]], input.size(), 0, chunk.size());
	}

	chunk.SetCardinality(chunk.size() + input.size());

	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalAdaptiveUnion::ParamsToString() const {
	return "";
}

} // namespace duckdb
