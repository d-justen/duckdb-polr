#include "duckdb/parallel/polar_enumeration_algo.hpp"
#include "duckdb/parallel/polar_config.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

unique_ptr<JoinEnumerationAlgo> JoinEnumerationAlgo::CreateEnumerationAlgo(ClientContext &context) {
	const auto algo = context.config.join_enumerator;

	switch (algo) {
	case JoinEnumerator::EXHAUSTIVE:
		return make_unique<ExhaustiveEnumeration>();
	case JoinEnumerator::EACH_LAST_ONCE:
		return make_unique<EachLastOnceEnumeration>();
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

void ExhaustiveEnumeration::GeneratePathsRecursive(unordered_map<idx_t, vector<idx_t>> &join_prerequisites,
                                                   vector<vector<idx_t>> &result, vector<idx_t> join_seq,
                                                   vector<idx_t> joins_left) {
	if (result.size() >= MAX_JOIN_ORDERS) {
		return;
	}

	for (idx_t i = 0; i < joins_left.size(); i++) {
		const idx_t join_idx = joins_left[i];
		if (!CanJoin(join_seq, join_idx, join_prerequisites)) {
			continue;
		}

		vector<idx_t> join_seq_new;
		join_seq_new.reserve(join_seq.size() + 1);
		join_seq_new.insert(join_seq_new.end(), join_seq.begin(), join_seq.end());
		join_seq_new.push_back(join_idx);
		vector<idx_t> joins_left_new(joins_left.begin(), joins_left.end());
		joins_left_new.erase(joins_left_new.begin() + i);

		if (joins_left_new.empty()) {
			result.push_back(join_seq_new);
		} else {
			GeneratePathsRecursive(join_prerequisites, result, move(join_seq_new), move(joins_left_new));
		}
	}
}

void ExhaustiveEnumeration::GenerateJoinOrders(const vector<idx_t> &hash_join_idxs,
                                               unordered_map<idx_t, vector<idx_t>> &dependencies,
                                               vector<vector<idx_t>> &join_orders) {
	// Exhaustive search
	vector<idx_t> joins_left;
	joins_left.reserve(hash_join_idxs.size());
	for (idx_t i = 0; i < hash_join_idxs.size(); i++) {
		joins_left.push_back(i);
	}

	join_orders.reserve(MAX_JOIN_ORDERS);
	GeneratePathsRecursive(dependencies, join_orders, vector<idx_t>(), joins_left);
}

void EachLastOnceEnumeration::GenerateJoinOrders(const vector<idx_t> &hash_join_idxs,
                                                 unordered_map<idx_t, vector<idx_t>> &dependencies,
                                                 vector<vector<idx_t>> &join_orders) {
	JoinEnumerationAlgo::GenerateJoinOrders(hash_join_idxs, dependencies, join_orders);
	auto &default_path = join_orders.front();

	for (idx_t i = 0; i < default_path.size() - 1; i++) {
		std::vector<idx_t> generated_path(default_path.cbegin(), default_path.cend());
		generated_path.erase(generated_path.begin() + i);

		if (!CanJoin(generated_path, i, dependencies)) {
			continue;
		}

		generated_path.push_back(i);
		join_orders.push_back(generated_path);
	}
}

} // namespace duckdb
