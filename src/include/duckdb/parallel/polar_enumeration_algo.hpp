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
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

#include <functional>
#include <random>
#include <stdlib.h>
#include <unordered_map>

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

class UncertainCardinalitySelector : public CandidateSelector {
public:
	idx_t SelectNextCandidate(const std::vector<idx_t> &join_idxs, const vector<PhysicalHashJoin *> &joins) override;

	idx_t ProjectUncertaintyRecursive(const PhysicalOperator &op, idx_t uncertainty_level);

	std::unordered_map<idx_t, idx_t> uncertainties;
};

class JoinEnumerationAlgo {
public:
	virtual ~JoinEnumerationAlgo() = default;
	// TODO: different/more descriptive form for relation dependencies
	virtual void GenerateJoinOrders(const vector<idx_t> &hash_join_idxs,
	                                unordered_map<idx_t, vector<idx_t>> &dependencies,
	                                const vector<PhysicalHashJoin *> &joins, vector<vector<idx_t>> &join_orders);

	bool CanJoin(vector<idx_t> &r, idx_t s, unordered_map<idx_t, vector<idx_t>> &dependencies);
	bool CanJoin(vector<idx_t> &r, vector<idx_t> &s, unordered_map<idx_t, vector<idx_t>> &dependencies);
	static unique_ptr<JoinEnumerationAlgo> CreateEnumerationAlgo(ClientContext &context);

	idx_t max_join_orders = 24;
};

struct JoinOrderNode {
	size_t id;
	PhysicalTableScan *scan;
	vector<JoinOrderNode> nested_join_order; // TODO: use pointers or so to stop excessive copying
	idx_t base_table_card = 0;
	bool predicate = false;
	bool unique = false;

	bool operator<(JoinOrderNode const &n) const {
		return id < n.id;
	}
};

class SelSampleEnumeration : public JoinEnumerationAlgo {
public:
	void GenerateJoinOrders(const vector<idx_t> &hash_join_idxs, unordered_map<idx_t, vector<idx_t>> &dependencies,
	                        const vector<PhysicalHashJoin *> &joins, vector<vector<idx_t>> &join_orders) override;

	vector<const JoinOrderNode *> DpSize(const vector<JoinOrderNode> &initial_join_order,
	                                     unordered_map<idx_t, vector<idx_t>> &dependencies);
	double CalculateCost(const vector<const JoinOrderNode *> &join_order);

private:
	std::map<vector<const JoinOrderNode *>, double> cost_map;
	std::map<set<const JoinOrderNode *>, double> card_map;
	std::map<set<const JoinOrderNode *>, vector<const JoinOrderNode *>> best_plans;
	std::mt19937 rng = std::mt19937(1337);
	std::uniform_real_distribution<double> dist;
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

	vector<idx_t> FindJoinCandidates(idx_t join_count, vector<idx_t> &predecessors,
	                                 unordered_map<idx_t, vector<idx_t>> &dependencies);

	const unique_ptr<CandidateSelector> selector;
};

} // namespace duckdb
