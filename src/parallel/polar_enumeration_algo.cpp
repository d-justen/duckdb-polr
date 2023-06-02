#include "duckdb/parallel/polar_enumeration_algo.hpp"
#include "duckdb/parallel/polar_config.hpp"
#include "duckdb/main/client_context.hpp"

#include <queue>

namespace duckdb {

idx_t RandomCandidateSelector::SelectNextCandidate(const std::vector<idx_t> &join_idxs,
                                                   const vector<PhysicalHashJoin *> &joins) {
	return join_idxs[rand() % join_idxs.size()];
}

idx_t MinCardinalitySelector::SelectNextCandidate(const std::vector<idx_t> &join_idxs,
                                                  const vector<PhysicalHashJoin *> &joins) {
	idx_t min_card = std::numeric_limits<idx_t>::max();
	idx_t selected_candidate = 0;
	for (idx_t i = 0; i < join_idxs.size(); i++) {
		auto *join = joins[join_idxs[i]];
		if (join->estimated_cardinality < min_card) {
			min_card = join->estimated_cardinality;
			selected_candidate = join_idxs[i];
		}
	}

	return selected_candidate;
}

unique_ptr<JoinEnumerationAlgo> JoinEnumerationAlgo::CreateEnumerationAlgo(ClientContext &context) {
	const auto algo = context.config.join_enumerator;

	switch (algo) {
	case JoinEnumerator::DFS_RANDOM:
		return make_unique<DFSEnumeration>(make_unique<RandomCandidateSelector>());
	case JoinEnumerator::DFS_MIN_CARD:
		return make_unique<DFSEnumeration>(make_unique<MinCardinalitySelector>());
	case JoinEnumerator::BFS_RANDOM:
		return make_unique<BFSEnumeration>(make_unique<RandomCandidateSelector>());
	case JoinEnumerator::BFS_MIN_CARD:
		return make_unique<BFSEnumeration>(make_unique<MinCardinalitySelector>());
	case JoinEnumerator::EACH_LAST_ONCE:
		return make_unique<EachLastOnceEnumeration>();
	case JoinEnumerator::EACH_FIRST_ONCE:
		return make_unique<EachFirstOnceEnumeration>();
	default:
		return nullptr;
	}
}

bool JoinEnumerationAlgo::CanJoin(vector<idx_t> &r, idx_t s, unordered_map<idx_t, vector<idx_t>> &dependencies) {
	auto &prereq = dependencies[s];
	for (const auto required_relation : prereq) {
		auto has_relation = std::find(r.begin(), r.end(), required_relation);
		if (has_relation == r.end()) {
			return false;
		}
	}
	return true;
}

void JoinEnumerationAlgo::GenerateJoinOrders(const vector<idx_t> &hash_join_idxs,
                                             unordered_map<idx_t, vector<idx_t>> &dependencies,
                                             const vector<PhysicalHashJoin *> &joins,
                                             vector<vector<idx_t>> &join_orders) {
	// Just generate the default join order
	std::vector<idx_t> default_path;
	default_path.reserve(hash_join_idxs.size());
	for (idx_t i = 0; i < hash_join_idxs.size(); i++) {
		default_path.push_back(i);
	}

	join_orders.reserve(MAX_JOIN_ORDERS);
	join_orders.push_back(default_path);
}

void DFSEnumeration::GeneratePathsRecursive(const vector<PhysicalHashJoin *> &joins,
                                            unordered_map<idx_t, vector<idx_t>> &join_prerequisites,
                                            vector<vector<idx_t>> &result, vector<idx_t> join_seq,
                                            vector<idx_t> joins_left) {
	if (result.size() >= MAX_JOIN_ORDERS) {
		return;
	}

	for (idx_t i = 0; i < joins_left.size(); i++) {
		// TODO: Build map or so to lookup join partners
		vector<idx_t> candidates;
		candidates.reserve(joins_left.size());

		for (auto join_idx : joins_left) {
			if (CanJoin(join_seq, join_idx, join_prerequisites)) {
				candidates.push_back(join_idx);
			}
		}

		const idx_t join_idx = selector->SelectNextCandidate(candidates, joins);

		vector<idx_t> join_seq_new;
		join_seq_new.reserve(join_seq.size() + 1);
		join_seq_new.insert(join_seq_new.end(), join_seq.begin(), join_seq.end());
		join_seq_new.push_back(join_idx);

		if (joins_left.size() == 1) {
			result.push_back(join_seq_new);
		} else {
			vector<idx_t> joins_left_new;
			joins_left_new.reserve(joins_left.size() - 1);
			for (auto j : joins_left) {
				if (j == join_idx) {
					continue;
				}
				joins_left_new.push_back(j);
			}

			GeneratePathsRecursive(joins, join_prerequisites, result, move(join_seq_new), move(joins_left_new));
		}
	}
}

void DFSEnumeration::GenerateJoinOrders(const vector<idx_t> &hash_join_idxs,
                                        unordered_map<idx_t, vector<idx_t>> &dependencies,
                                        const vector<PhysicalHashJoin *> &joins, vector<vector<idx_t>> &join_orders) {
	// Exhaustive search
	vector<idx_t> joins_left;
	joins_left.reserve(hash_join_idxs.size());
	for (idx_t i = 0; i < hash_join_idxs.size(); i++) {
		joins_left.push_back(i);
	}

	join_orders.reserve(MAX_JOIN_ORDERS);
	GeneratePathsRecursive(joins, dependencies, join_orders, vector<idx_t>(), joins_left);
}

void EachLastOnceEnumeration::GenerateJoinOrders(const vector<idx_t> &hash_join_idxs,
                                                 unordered_map<idx_t, vector<idx_t>> &dependencies,
                                                 const vector<PhysicalHashJoin *> &joins,
                                                 vector<vector<idx_t>> &join_orders) {
	JoinEnumerationAlgo::GenerateJoinOrders(hash_join_idxs, dependencies, joins, join_orders);
	auto &default_path = join_orders.front();
	for (idx_t i = 0; i < default_path.size() - 1; i++) {
		vector<idx_t> generated_path;
		generated_path.reserve(default_path.size());

		for (idx_t j = 0; j < default_path.size(); j++) {
			if (j == i) {
				continue;
			}

			if (!CanJoin(generated_path, default_path[j], dependencies)) {
				break;
			}

			generated_path.push_back(default_path[j]);
		}

		if (generated_path.size() == default_path.size() - 1 &&
		    CanJoin(generated_path, default_path[i], dependencies)) {
			generated_path.push_back(default_path[i]);
			join_orders.push_back(generated_path);
		}
	}
}

void EachFirstOnceEnumeration::GenerateJoinOrders(const vector<idx_t> &hash_join_idxs,
                                                  unordered_map<idx_t, vector<idx_t>> &dependencies,
                                                  const vector<PhysicalHashJoin *> &joins,
                                                  vector<vector<idx_t>> &join_orders) {
	JoinEnumerationAlgo::GenerateJoinOrders(hash_join_idxs, dependencies, joins, join_orders);
	auto &default_path = join_orders.front();
	for (idx_t i = 1; i < default_path.size(); i++) {
		vector<idx_t> generated_path;
		generated_path.reserve(default_path.size());

		if (!CanJoin(generated_path, default_path[i], dependencies)) {
			continue;
		}

		generated_path.push_back(default_path[i]);

		for (idx_t j = 0; j < default_path.size(); j++) {
			if (j == i) {
				continue;
			}

			if (!CanJoin(generated_path, default_path[j], dependencies)) {
				break;
			}

			generated_path.push_back(default_path[j]);
		}

		if (generated_path.size() == default_path.size()) {
			join_orders.push_back(generated_path);
		}
	}
}

void BFSEnumeration::GenerateJoinOrders(const vector<idx_t> &hash_join_idxs,
                                        unordered_map<idx_t, vector<idx_t>> &dependencies,
                                        const vector<PhysicalHashJoin *> &joins, vector<vector<idx_t>> &join_orders) {
	vector<idx_t> joined;
	vector<idx_t> next_layer;
	next_layer.reserve(hash_join_idxs.size());

	for (idx_t i = 0; i < hash_join_idxs.size(); i++) {
		if (CanJoin(joined, i, dependencies)) {
			next_layer.push_back(i);
		}
	}

	std::queue<pair<vector<idx_t>, vector<idx_t>>> q;
	q.emplace(joined, next_layer);

	while (!q.empty()) {
		auto &next = q.front();
		joined = next.first;
		auto &candidates = next.second;

		// Pick next candidate
		idx_t next_join_idx = selector->SelectNextCandidate(candidates, joins);
		candidates.erase(std::find(candidates.begin(), candidates.end(), next_join_idx));
		joined.push_back(next_join_idx);

		if (candidates.empty()) {
			q.pop();
		}

		if (joined.size() == hash_join_idxs.size()) {
			join_orders.push_back(joined);
			continue;
		}

		vector<idx_t> remaining_joins(hash_join_idxs.size());
		std::iota(remaining_joins.begin(), remaining_joins.end(), 0);

		for (auto j : joined) {
			remaining_joins.erase(std::find(remaining_joins.begin(), remaining_joins.end(), j));
		}

		// Pick one
		idx_t remaining_count = remaining_joins.size();
		for (idx_t i = 0; i < remaining_count; i++) {
			next_layer.clear();
			for (auto join_idx : remaining_joins) {
				if (CanJoin(joined, join_idx, dependencies)) {
					next_layer.push_back(join_idx);
				}
			}

			if (next_layer.empty()) {
				break;
			}

			next_join_idx = selector->SelectNextCandidate(next_layer, joins);
			next_layer.erase(std::find(next_layer.begin(), next_layer.end(), next_join_idx));
			remaining_joins.erase(std::find(remaining_joins.begin(), remaining_joins.end(), next_join_idx));

			if (!next_layer.empty()) {
				q.emplace(joined, next_layer);
			}

			joined.push_back(next_join_idx);
		}

		if (joined.size() == hash_join_idxs.size()) {
			join_orders.push_back(joined);
			if (join_orders.size() >= MAX_JOIN_ORDERS) {
				return;
			}
		}
	}
}

} // namespace duckdb
