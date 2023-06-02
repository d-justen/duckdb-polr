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

#include <functional>
#include <stdlib.h>

namespace duckdb {

class CandidateSelector {
public:
	virtual ~CandidateSelector() = default;
	virtual idx_t SelectNextCandidate(const std::vector<idx_t> &join_idxs,
	                                  const vector<PhysicalHashJoin *> &joins_p) = 0;
};

class RandomCandidateSelector : public CandidateSelector {
public:
	idx_t SelectNextCandidate(const std::vector<idx_t> &join_idxs, const vector<PhysicalHashJoin *> &joins) override;
};

class MinCardinalitySelector : public CandidateSelector {
public:
	idx_t SelectNextCandidate(const std::vector<idx_t> &join_idxs, const vector<PhysicalHashJoin *> &joins) override;
};

class JoinEnumerationAlgo {
public:
	virtual ~JoinEnumerationAlgo() = default;
	// TODO: different/more descriptive form for relation dependencies
	virtual void GenerateJoinOrders(const vector<idx_t> &hash_join_idxs,
	                                unordered_map<idx_t, vector<idx_t>> &dependencies,
	                                const vector<PhysicalHashJoin *> &joins, vector<vector<idx_t>> &join_orders);

	bool CanJoin(vector<idx_t> &r, idx_t s, unordered_map<idx_t, vector<idx_t>> &dependencies);
	static unique_ptr<JoinEnumerationAlgo> CreateEnumerationAlgo(ClientContext &context);

	const idx_t MAX_JOIN_ORDERS = 24;
};

class DFSEnumeration : public JoinEnumerationAlgo {
public:
	explicit DFSEnumeration(unique_ptr<CandidateSelector> selector_p) : selector(move(selector_p)) {
	}
	void GenerateJoinOrders(const vector<idx_t> &hash_join_idxs, unordered_map<idx_t, vector<idx_t>> &dependencies,
	                        const vector<PhysicalHashJoin *> &joins, vector<vector<idx_t>> &join_orders) override;
	void GeneratePathsRecursive(const vector<PhysicalHashJoin *> &joins,
	                            unordered_map<idx_t, vector<idx_t>> &join_prerequisites, vector<vector<idx_t>> &result,
	                            vector<idx_t> join_seq, vector<idx_t> joins_left);

	const unique_ptr<CandidateSelector> selector;
};

class EachLastOnceEnumeration : public JoinEnumerationAlgo {
public:
	void GenerateJoinOrders(const vector<idx_t> &hash_join_idxs, unordered_map<idx_t, vector<idx_t>> &dependencies,
	                        const vector<PhysicalHashJoin *> &joins, vector<vector<idx_t>> &join_orders) override;
};

class EachFirstOnceEnumeration : public JoinEnumerationAlgo {
public:
	void GenerateJoinOrders(const vector<idx_t> &hash_join_idxs, unordered_map<idx_t, vector<idx_t>> &dependencies,
	                        const vector<PhysicalHashJoin *> &joins, vector<vector<idx_t>> &join_orders) override;
};

class BFSEnumeration : public JoinEnumerationAlgo {
public:
	explicit BFSEnumeration(unique_ptr<CandidateSelector> selector_p) : selector(move(selector_p)) {
	}

	void GenerateJoinOrders(const vector<idx_t> &hash_join_idxs, unordered_map<idx_t, vector<idx_t>> &dependencies,
	                        const vector<PhysicalHashJoin *> &joins, vector<vector<idx_t>> &join_orders) override;

	const unique_ptr<CandidateSelector> selector;
};

} // namespace duckdb
