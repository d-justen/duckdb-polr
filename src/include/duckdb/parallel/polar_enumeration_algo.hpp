//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/polar_enumeration_algo.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/join_enumerator.hpp"

namespace duckdb {

class JoinEnumerationAlgo {
public:
	virtual ~JoinEnumerationAlgo() = default;
	// TODO: different/more descriptive form for relation dependencies
	virtual void GenerateJoinOrders(const vector<idx_t> &hash_join_idxs,
	                                unordered_map<idx_t, vector<idx_t>> &dependencies,
	                                vector<vector<idx_t>> &join_orders);
	bool CanJoin(vector<idx_t> &r, idx_t s, unordered_map<idx_t, vector<idx_t>> &dependencies);
	static unique_ptr<JoinEnumerationAlgo> CreateEnumerationAlgo(ClientContext &context);
	const idx_t MAX_JOIN_ORDERS = 24;
};

class ExhaustiveEnumeration : public JoinEnumerationAlgo {
	void GenerateJoinOrders(const vector<idx_t> &hash_join_idxs, unordered_map<idx_t, vector<idx_t>> &dependencies,
	                        vector<vector<idx_t>> &join_orders) override;
	void GeneratePathsRecursive(unordered_map<idx_t, vector<idx_t>> &join_prerequisites, vector<vector<idx_t>> &result,
	                            vector<idx_t> join_seq, vector<idx_t> joins_left);
};

class EachLastOnceEnumeration : public JoinEnumerationAlgo {
	void GenerateJoinOrders(const vector<idx_t> &hash_join_idxs, unordered_map<idx_t, vector<idx_t>> &dependencies,
	                        vector<vector<idx_t>> &join_orders) override;
};

} // namespace duckdb
