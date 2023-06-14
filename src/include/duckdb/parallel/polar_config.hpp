//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/polar_config.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/polr/physical_adaptive_union.hpp"
#include "duckdb/execution/operator/polr/physical_multiplexer.hpp"
#include "duckdb/parallel/polar_enumeration_algo.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

class Pipeline;

class POLARConfig {
public:
	POLARConfig(Pipeline *pipeline_p, unique_ptr<JoinEnumerationAlgo> enumerator_p);
	bool GenerateJoinOrders();

	const Pipeline *pipeline;
	const unique_ptr<JoinEnumerationAlgo> enumerator;

	vector<PhysicalOperator *> operators;
	vector<PhysicalHashJoin *> joins;
	vector<vector<idx_t>> join_paths;
	vector<vector<std::map<idx_t, idx_t>>> left_expression_bindings;
	idx_t multiplexer_idx;
	std::unique_ptr<PhysicalMultiplexer> multiplexer;
	std::unique_ptr<PhysicalAdaptiveUnion> adaptive_union;
	bool measure_polr_pipeline;
	bool log_tuples_routed;
	unique_ptr<GlobalSourceState> source_state;
	vector<idx_t> hash_join_idxs;

	unique_ptr<vector<unique_ptr<Pipeline>>> backpressure_pipelines;
};

} // namespace duckdb
