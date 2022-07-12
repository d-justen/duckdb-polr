#include "duckdb/execution/operator/polr/physical_adaptive_union.hpp"

#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <algorithm>
#include <chrono>
#include <map>

namespace duckdb {

PhysicalAdaptiveUnion::PhysicalAdaptiveUnion(vector<LogicalType> types, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::ADAPTIVE_UNION, move(types), estimated_cardinality) {
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
	vector<idx_t> current_join_path = *context.thread.current_join_path;

	// TODO: we should only do this once
	idx_t num_columns_from_left = input.ColumnCount();
	for (idx_t i = 0; i < context.thread.join_build_types.size(); i++) {
		num_columns_from_left -= context.thread.join_build_types[i].size();
	}

	vector<idx_t> offsets;
	offsets.push_back(num_columns_from_left);

	for (idx_t i = 0; i < context.thread.join_build_types.size(); i++) {
		offsets.push_back(offsets.back() + context.thread.join_build_types[i].size());
	}

	// TODO: We probably don't want to copy here but allow the adaptive union to produce multiple chunks

	// Nice, chunk is empty, let's just reference the columns in the right order
	if (chunk.size() == 0) {
		chunk.SetCapacity(input);
		chunk.SetCardinality(input);

		for (idx_t i = 0; i < offsets.front(); i++) {
			chunk.data[i].Reference(input.data[i]);
		}

		idx_t current_offset = input.ColumnCount() - 1;
		for (int i = current_join_path.size(); i > 0; i--) {
			idx_t join_idx = current_join_path[i - 1];

			idx_t target_columns_end = offsets[join_idx + 1];
			idx_t target_columns_begin = offsets[join_idx];

			for (idx_t j = target_columns_end - 1; j >= target_columns_begin; j--) {
				chunk.data[j].Reference(input.data[current_offset]);
				current_offset--;
			}
		}

		return OperatorResultType::NEED_MORE_INPUT;
	}

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

	idx_t last_offset = num_columns_from_left;

	for (idx_t i = 0; i < current_join_path.size(); i++) {
		idx_t target_offset_begin = offsets[current_join_path[i]];
		idx_t target_offset_end = offsets[current_join_path[i] + 1];

		for (idx_t j = target_offset_begin; j < target_offset_end; j++) {
			VectorOperations::Copy(input.data[last_offset], chunk.data[j], input.size(), 0, chunk.size());
			last_offset++;
		}
	}

	chunk.SetCardinality(chunk.size() + input.size());

	return OperatorResultType::NEED_MORE_INPUT;
}

string PhysicalAdaptiveUnion::ParamsToString() const {
	return "";
}

} // namespace duckdb
