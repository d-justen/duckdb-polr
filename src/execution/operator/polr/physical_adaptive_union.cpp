#include "duckdb/execution/operator/polr/physical_adaptive_union.hpp"

#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>
#include <chrono>
#include <map>

namespace duckdb {

PhysicalAdaptiveUnion::PhysicalAdaptiveUnion(vector<LogicalType> types, idx_t num_columns_from_left_p,
                                             vector<idx_t> num_columns_per_join_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::ADAPTIVE_UNION, move(types), estimated_cardinality),
      num_columns_from_left(num_columns_from_left_p),
      num_columns_per_join(move(num_columns_per_join_p)) {
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
	D_ASSERT(context.thread.current_join_path); // TODO: Store this in op_state?
	vector<idx_t>& current_join_path = *context.thread.current_join_path;

	// TODO: We probably don't want to copy here but allow the adaptive union to produce multiple chunks
	// Nice, chunk is empty, let's just reference the columns in the right order
	if (chunk.size() == 0) {
		chunk.SetCapacity(input);
		chunk.SetCardinality(input);

		// Reference columns from left side
		for (idx_t i = 0; i < num_columns_from_left; i++) {
			chunk.data[i].Reference(input.data[i]);
		}

		// Reference build columns in the original join order
		idx_t current_offset = num_columns_from_left;
		for (idx_t i = 0; i < current_join_path.size(); i++) {
			idx_t join_idx = current_join_path[i];

			// Current offset -> offset + num_build_columns is the source range
			idx_t num_build_columns = join_idx == 0 ? num_columns_per_join[join_idx] - num_columns_from_left
			                                        : num_columns_per_join[join_idx] - num_columns_per_join[join_idx - 1];

			idx_t target_columns_begin = join_idx == 0 ? num_columns_from_left : num_columns_per_join[join_idx - 1];

			// The difference between num_columns_per_join[i] and i-1 should be == num_build_columns
			D_ASSERT(target_columns_begin + num_build_columns == num_columns_per_join[join_idx]);

			for (idx_t j = target_columns_begin; j < num_columns_per_join[i]; j++) {
				chunk.data[j].Reference(input.data[current_offset]);
				current_offset++;
			}
		}

		return OperatorResultType::NEED_MORE_INPUT;
	}

	D_ASSERT(0 == 1); // The copying path we dont want

	idx_t new_chunk_size = chunk.size() + input.size();
	if (new_chunk_size > chunk.capacity) {
		for (idx_t i = 0; i < chunk.data.size(); i++) {
			chunk.data[i].Resize(chunk.size(), new_chunk_size);
		}
		chunk.capacity = new_chunk_size;
	}

	for (idx_t i = 0; i < num_columns_from_left; i++) {
		VectorOperations::Copy(input.data[i], chunk.data[i], input.size(), 0, chunk.size());
	}

	/*
	idx_t last_offset = num_columns_from_left;
		for (idx_t i = 0; i < current_join_path.size(); i++) {
			idx_t target_offset_begin = offsets[current_join_path[i]];
			idx_t target_offset_end = offsets[current_join_path[i] + 1];

			for (idx_t j = target_offset_begin; j < target_offset_end; j++) {
				VectorOperations::Copy(input.data[last_offset], chunk.data[j], input.size(), 0, chunk.size());
				last_offset++;
			}
		}
	 */

	chunk.SetCardinality(chunk.size() + input.size());

	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalAdaptiveUnion::ParamsToString() const {
	return "";
}

} // namespace duckdb
