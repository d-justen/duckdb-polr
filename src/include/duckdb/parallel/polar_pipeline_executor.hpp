//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/polar_pipeline_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/common/stack.hpp"

#include <functional>

namespace duckdb {
class Executor;

class POLARPipelineExecutor : public PipelineExecutor {

public:
	POLARPipelineExecutor(ClientContext &context, Pipeline &pipeline);

	bool Execute(idx_t max_chunks) override;
	void PushFinalize() override;

	void RunPath(DataChunk &chunk, DataChunk &result, idx_t start_idx = 0);

private:
	OperatorResultType FlushInProcessJoins(DataChunk &result, bool &did_flush);
	OperatorResultType FlushJoinCaches(DataChunk &result, bool &did_flush);

	vector<vector<unique_ptr<DataChunk>>> join_intermediate_chunks;
	vector<vector<unique_ptr<OperatorState>>> join_intermediate_states;
	OperatorState *multiplexer_state;
	unique_ptr<OperatorState> adaptive_union_state;
	vector<vector<unique_ptr<DataChunk>>> cached_join_chunks;
	stack<idx_t> in_process_joins;
	unique_ptr<DataChunk> mpx_output_chunk;
	idx_t num_intermediates_produced = 0;

private:
	OperatorResultType Execute(DataChunk &input, DataChunk &result, idx_t initial_index = 0) override;
	void CacheJoinChunk(DataChunk &current_chunk, idx_t operator_idx);
};

} // namespace duckdb
