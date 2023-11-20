#include "duckdb/parallel/polar_enumeration_algo.hpp"

#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/polar_config.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"

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

idx_t UncertainCardinalitySelector::ProjectUncertaintyRecursive(const PhysicalOperator &op, idx_t uncertainty_level) {
	if (op.type == PhysicalOperatorType::TABLE_SCAN) {
		auto &table_filters = ((PhysicalTableScan &)op).table_filters;
		if (table_filters && !table_filters->filters.empty()) {
			uncertainty_level++;
		}
	} else if (op.children.size() > 1) {
		// i.e. join
		uncertainty_level++;
	} else if (op.type == PhysicalOperatorType::FILTER) {
		uncertainty_level++;
	}

	idx_t max_uncertainty = uncertainty_level;
	for (auto &child : op.children) {
		idx_t u = ProjectUncertaintyRecursive(*child, uncertainty_level);
		if (u > max_uncertainty) {
			max_uncertainty = u;
		}
	}

	return max_uncertainty;
}

idx_t UncertainCardinalitySelector::SelectNextCandidate(const std::vector<idx_t> &join_idxs,
                                                        const vector<PhysicalHashJoin *> &joins) {
	idx_t min_card = std::numeric_limits<idx_t>::max();
	idx_t selected_candidate = 0;

	for (auto join_idx : join_idxs) {
		auto *join = joins[join_idx];

		if (uncertainties.find(join_idx) == uncertainties.end()) {
			idx_t uncertainty_level = ProjectUncertaintyRecursive(*join->children[1], 1);
			uncertainties[join_idx] = uncertainty_level * join->estimated_cardinality;
		}

		if (uncertainties[join_idx] < min_card) {
			min_card = uncertainties[join_idx];
			selected_candidate = join_idx;
		}
	}

	return selected_candidate;
}

unique_ptr<JoinEnumerationAlgo> JoinEnumerationAlgo::CreateEnumerationAlgo(ClientContext &context) {
	const auto enumerator = context.config.join_enumerator;
	unique_ptr<JoinEnumerationAlgo> algo;

	switch (enumerator) {
	case JoinEnumerator::DFS_RANDOM:
		algo = make_unique<DFSEnumeration>(make_unique<RandomCandidateSelector>());
		break;
	case JoinEnumerator::DFS_MIN_CARD:
		algo = make_unique<DFSEnumeration>(make_unique<MinCardinalitySelector>());
		break;
	case JoinEnumerator::DFS_UNCERTAIN:
		algo = make_unique<DFSEnumeration>(make_unique<UncertainCardinalitySelector>());
		break;
	case JoinEnumerator::BFS_RANDOM:
		algo = make_unique<BFSEnumeration>(make_unique<RandomCandidateSelector>());
		break;
	case JoinEnumerator::BFS_MIN_CARD:
		algo = make_unique<BFSEnumeration>(make_unique<MinCardinalitySelector>());
		break;
	case JoinEnumerator::BFS_UNCERTAIN:
		algo = make_unique<BFSEnumeration>(make_unique<UncertainCardinalitySelector>());
		break;
	case JoinEnumerator::EACH_LAST_ONCE:
		algo = make_unique<EachLastOnceEnumeration>();
		break;
	case JoinEnumerator::EACH_FIRST_ONCE:
		algo = make_unique<EachFirstOnceEnumeration>();
		break;
	case JoinEnumerator::SAMPLE:
		algo = make_unique<SelSampleEnumeration>();
		break;
	default:
		D_ASSERT(algo);
	}

	algo->max_join_orders = context.config.max_join_orders;
	return algo;
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

bool JoinEnumerationAlgo::CanJoin(vector<idx_t> &r, vector<idx_t> &s,
                                  unordered_map<idx_t, vector<idx_t>> &dependencies) {
	for (auto &si : s) {
		if (CanJoin(r, si, dependencies)) {
			return true;
		}
	}
	return false;
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

	join_orders.reserve(max_join_orders);
	join_orders.push_back(default_path);
}

void DFSEnumeration::GeneratePathsRecursive(const vector<PhysicalHashJoin *> &joins,
                                            unordered_map<idx_t, vector<idx_t>> &join_prerequisites,
                                            vector<vector<idx_t>> &result, vector<idx_t> join_seq,
                                            vector<idx_t> joins_left) {
	if (result.size() >= max_join_orders) {
		return;
	}

	vector<idx_t> candidates;
	candidates.reserve(joins_left.size());

	for (auto join_idx : joins_left) {
		if (CanJoin(join_seq, join_idx, join_prerequisites)) {
			candidates.push_back(join_idx);
		}
	}

	idx_t num_relations = candidates.size();
	for (idx_t i = 0; i < num_relations; i++) {
		const idx_t join_idx = selector->SelectNextCandidate(candidates, joins);
		candidates.erase(std::find(candidates.begin(), candidates.end(), join_idx));

		vector<idx_t> join_seq_new;
		join_seq_new.reserve(join_seq.size() + 1);
		join_seq_new.insert(join_seq_new.end(), join_seq.begin(), join_seq.end());
		join_seq_new.push_back(join_idx);

		if (joins_left.size() == 1) {
			result.push_back(join_seq_new);
		} else {
			vector<idx_t> joins_left_new(joins_left.begin(), joins_left.end());
			joins_left_new.erase(std::find(joins_left_new.begin(), joins_left_new.end(), join_idx));
			GeneratePathsRecursive(joins, join_prerequisites, result, move(join_seq_new), move(joins_left_new));
		}
	}
}

bool ExtractInfoLinear(PhysicalOperator *op, JoinOrderNode &node) {
	D_ASSERT(op);
	while (true) {
		if (op->children.size() > 1) {
			if (op->type == PhysicalOperatorType::HASH_JOIN) {
				auto *hj = (PhysicalHashJoin *)op;
				if (hj->join_type == JoinType::SEMI || hj->join_type == JoinType::ANTI ||
				    hj->join_type == JoinType::MARK) {
					node.predicate = true;
					op = &*op->children[0];
					continue;
				}
			}
			return false;
		}
		if (op->children.size() == 1) {
			if (op->type == PhysicalOperatorType::FILTER) {
				node.predicate = true;
			}
			op = &*op->children[0];
		} else {
			if (op->type == PhysicalOperatorType::COLUMN_DATA_SCAN) {
				auto *scan = (PhysicalColumnDataScan *)op;
				node.base_table_card = scan->collection->Count();
				node.unique = true;
				return true;
			}
			D_ASSERT(op->type == PhysicalOperatorType::TABLE_SCAN);
			auto *scan = (PhysicalTableScan *)op;
			node.scan = scan;
			auto &table_filters = scan->table_filters;
			if (table_filters && !table_filters->filters.empty()) {
				node.predicate = true;
			}
			auto *tbl_entry = TableScanFunction::GetTableEntry(scan->function, &*scan->bind_data);
			node.base_table_card = tbl_entry->storage->info->cardinality;

			for (auto &constraint : tbl_entry->constraints) {
				if (constraint->type == ConstraintType::UNIQUE) {
					auto &unique_constraint = (UniqueConstraint &)*constraint;
					for (auto idx : scan->column_ids) { // TODO: specific column
						if (unique_constraint.index == idx) {
							node.unique = true;
							break;
						}
					}
				}
				if (node.unique) {
					break;
				}
			}
			break;
		}
	}
	return true;
}

void CreateJoinOrderNodes(PhysicalOperator *op, vector<JoinOrderNode> &nodes, idx_t idx = 0) {
	vector<PhysicalOperator *> joins;

	while (!op->children.empty()) {
		if (op->children.size() == 2) {
			joins.push_back(op);
		} else {
			if (idx == 0) { // TODO: instead check if op.is_polr_root_join
				break;      // TODO: change for join changes with interleaved filters/projections
			}
		}
		op = &*op->children[0];
	}
	std::reverse(joins.begin(), joins.end());
	nodes.reserve(joins.size() + 1);

	JoinOrderNode source;
	source.id = idx + joins.size();
	if (!ExtractInfoLinear(&*joins[0]->children[0], source)) {
		CreateJoinOrderNodes(&*joins[0]->children[0], source.nested_join_order, joins.size() * source.id + 1);
	}
	nodes.push_back(source);

	for (auto *join : joins) {
		JoinOrderNode node;
		node.id = idx++;
		if (!ExtractInfoLinear(&*join->children[1], node)) {
			CreateJoinOrderNodes(&*join->children[1], node.nested_join_order, joins.size() * idx + 1);
		}
		nodes.push_back(node);
	}
}

void GeneratePermutations(const vector<idx_t> &input, idx_t r, vector<idx_t> data, idx_t d_idx, idx_t i_idx,
                          vector<vector<idx_t>> &permutations) {
	if (d_idx == r) {
		vector<idx_t> tmp(r);
		for (idx_t j = 0; j < r; j++) {
			tmp[j] = data[j];
		}
		permutations.push_back(tmp);
		return;
	}
	if (i_idx >= input.size()) {
		return;
	}

	data[d_idx] = input[i_idx];
	GeneratePermutations(input, r, data, d_idx + 1, i_idx + 1, permutations);
	GeneratePermutations(input, r, data, d_idx, i_idx + 1, permutations);
}

// TODO: emit vector of sets/sorted vectors for better comparability
vector<vector<idx_t>> GeneratePermutations(idx_t n, idx_t r) {
	vector<idx_t> input(n);
	for (idx_t i = 0; i < n; i++) {
		input[i] = i;
	}

	vector<idx_t> data(r);
	vector<vector<idx_t>> permutations;
	GeneratePermutations(input, r, data, 0, 0, permutations);
	return permutations;
}

bool Disjoint(const vector<idx_t> &r, const idx_t s) {
	for (const auto r1 : r) {
		if (r1 == s) {
			return false;
		}
	}
	return true;
}

vector<const JoinOrderNode *> SelSampleEnumeration::DpSize(const vector<JoinOrderNode> &initial_join_order,
                                                           unordered_map<idx_t, vector<idx_t>> &dependencies) {
	set<const JoinOrderNode *> join_nodes;
	for (idx_t i = 1; i < initial_join_order.size(); i++) {
		join_nodes.insert(&initial_join_order[i]);
		best_plans[{&initial_join_order[i]}] = {&initial_join_order[i]};
	}

	vector<idx_t> empty;

	for (idx_t s = 1; s < join_nodes.size(); s++) {
		auto permutations_s1 = GeneratePermutations(join_nodes.size(), s);

		for (auto &p_s1 : permutations_s1) {
			for (idx_t p_s2 = 0; p_s2 < join_nodes.size(); p_s2++) {
				if (!Disjoint(p_s1, p_s2)) {
					continue;
				}
				if (!CanJoin(empty, p_s1, dependencies) || !CanJoin(p_s1, p_s2, dependencies)) {
					continue;
				}
				set<const JoinOrderNode *> new_set;
				vector<const JoinOrderNode *> new_plan;
				new_plan.reserve(p_s1.size() + 2);
				new_plan.push_back(&initial_join_order.front());
				for (auto idx : p_s1) {
					new_set.insert(&initial_join_order[idx + 1]); // TODO: join nodes set to vec?
					new_plan.push_back(&initial_join_order[idx + 1]);
				}
				if (best_plans.find(new_set) == best_plans.cend()) {
					continue;
				}
				new_set.insert(&initial_join_order[p_s2 + 1]);
				new_plan.push_back(&initial_join_order[p_s2 + 1]);
				auto best_plan = best_plans.find(new_set);
				if (best_plan == best_plans.cend() || CalculateCost(new_plan) < CalculateCost(best_plan->second)) {
					best_plans[new_set] = new_plan;
				}
			}
		}
	}

	return best_plans[join_nodes];
}

double SelSampleEnumeration::CalculateCost(const vector<const JoinOrderNode *> &join_order) {
	auto entry = cost_map.find(join_order);
	if (entry != cost_map.cend()) {
		return entry->second;
	}

	if (join_order.size() == 1) {
		auto *node = join_order.front();
		if (!node->nested_join_order.empty()) {
			vector<const JoinOrderNode *> nodes;
			for (auto &n : node->nested_join_order) {
				nodes.push_back(&n);
				CalculateCost(nodes);
				cost_map[nodes] = 0;
			}
		} else {
			double card = node->base_table_card;
			if (node->predicate) {
				card *= dist(rng);
			}

			card_map[std::set<const JoinOrderNode *>(join_order.cbegin(), join_order.cend())] = card;
			cost_map[join_order] = 0;
		}
	} else {
		auto lhs = set<const JoinOrderNode *>(join_order.cbegin(), join_order.cend() - 1);
		auto lhs_ordered = vector<const JoinOrderNode *>(join_order.begin(), join_order.cend() - 1);
		auto rhs = set<const JoinOrderNode *> {join_order.back()};

		if (card_map.find(lhs) == card_map.cend()) {
			CalculateCost(lhs_ordered);
		}
		if (card_map.find(rhs) == card_map.cend()) {
			CalculateCost(vector<const JoinOrderNode *> {join_order.back()});
		}
		double card = card_map[lhs];

		if (join_order.back()->unique) {
			// TODO: check if unique on the other side
			card *= dist(rng);
		} else {
			card *= card_map[rhs] * dist(rng);
		}
		lhs.insert(join_order.back());
		card_map[lhs] = card;
		cost_map[join_order] = cost_map[lhs_ordered] + card;
	}

	return cost_map[join_order];
}

void SelSampleEnumeration::GenerateJoinOrders(const vector<idx_t> &hash_join_idxs,
                                              unordered_map<idx_t, vector<idx_t>> &dependencies,
                                              const vector<PhysicalHashJoin *> &joins,
                                              vector<vector<idx_t>> &join_orders) {
	idx_t SAMPLE_COUNT = max_join_orders;
	// Build Join Order Nodes
	vector<JoinOrderNode> nodes;
	CreateJoinOrderNodes(joins.back(), nodes);

	set<vector<idx_t>> unique_join_orders;

	for (idx_t i = 0; i < SAMPLE_COUNT; i++) {
		auto join_nodes = DpSize(nodes, dependencies);
		vector<idx_t> join_order(join_nodes.size() - 1);
		for (idx_t j = 1; j < join_nodes.size(); j++) {
			join_order[j - 1] = join_nodes[j]->id;
		}
		unique_join_orders.insert(join_order);
		cost_map.clear();
		card_map.clear();
		best_plans.clear();
	}

	vector<idx_t> inital_join_order(joins.size());
	std::iota(inital_join_order.begin(), inital_join_order.end(), 0);

	if (unique_join_orders.find(inital_join_order) != unique_join_orders.cend()) {
		unique_join_orders.erase(inital_join_order);
	}

	join_orders.reserve(unique_join_orders.size() + 1);
	join_orders.push_back(inital_join_order);
	join_orders.insert(join_orders.cend(), unique_join_orders.cbegin(), unique_join_orders.cend());
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

	join_orders.reserve(max_join_orders + 1);
	GeneratePathsRecursive(joins, dependencies, join_orders, vector<idx_t>(), joins_left);

	// TODO: Deduplicate this code in method
	bool contains_original_join_order = false;
	idx_t original_join_order_idx = 0;
	for (idx_t i = 0; i < join_orders.size(); i++) {
		auto &join_order = join_orders[i];
		bool is_original_join_order = true;
		for (idx_t j = 0; j < join_order.size(); j++) {
			if (join_order[j] != j) {
				is_original_join_order = false;
				break;
			}
		}
		if (is_original_join_order) {
			contains_original_join_order = true;
			original_join_order_idx = i;
			break;
		}
	}

	if (!contains_original_join_order) {
		vector<idx_t> original_join_order(hash_join_idxs.size());
		std::iota(original_join_order.begin(), original_join_order.end(), 0);
		join_orders.insert(join_orders.begin(), original_join_order);
		if (join_orders.size() > max_join_orders) {
			join_orders.erase(join_orders.end() - 1);
		}
	} else if (original_join_order_idx != 0) {
		auto original_join_order = join_orders[original_join_order_idx];
		join_orders.erase(join_orders.begin() + original_join_order_idx);
		join_orders.insert(join_orders.begin(), original_join_order);
	}
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

struct JoinCandidateEntry {
	idx_t level;
	idx_t candidate_idx;
	idx_t step;
	vector<idx_t> predecessors;
	idx_t candidate;

	friend bool operator<(JoinCandidateEntry const &left, JoinCandidateEntry const &right) {
		if (left.level == right.level) {
			if (left.candidate_idx == right.candidate_idx) {
				return left.step > right.step;
			}
			return left.candidate_idx > right.candidate_idx;
		}
		return left.level > right.level;
	}
};

vector<idx_t> BFSEnumeration::FindJoinCandidates(idx_t join_count, vector<idx_t> &predecessors,
                                                 unordered_map<idx_t, vector<idx_t>> &dependencies) {
	vector<bool> found_relation(join_count, false);

	for (auto predecessor : predecessors) {
		found_relation[predecessor] = true;
	}

	vector<idx_t> result;
	for (idx_t i = 0; i < found_relation.size(); i++) {
		if (!found_relation[i] && CanJoin(predecessors, i, dependencies)) {
			result.push_back(i);
		}
	}
	return result;
}

void BFSEnumeration::GenerateJoinOrders(const vector<idx_t> &hash_join_idxs,
                                        unordered_map<idx_t, vector<idx_t>> &dependencies,
                                        const vector<PhysicalHashJoin *> &joins, vector<vector<idx_t>> &join_orders) {
	std::priority_queue<JoinCandidateEntry> queue;

	vector<idx_t> empty_predecessors;
	empty_predecessors.reserve(hash_join_idxs.size());
	vector<idx_t> first_level_candidates = FindJoinCandidates(hash_join_idxs.size(), empty_predecessors, dependencies);
	first_level_candidates.reserve(hash_join_idxs.size());

	idx_t step = 0;
	idx_t num_initial_candidates = std::min(4, (int)first_level_candidates.size());
	for (idx_t i = 0; i < num_initial_candidates; i++) { // TODO: make constant
		idx_t next_join_idx = selector->SelectNextCandidate(first_level_candidates, joins);
		queue.push(JoinCandidateEntry {0, i, step, empty_predecessors, next_join_idx});
		first_level_candidates.erase(
		    std::find(first_level_candidates.begin(), first_level_candidates.end(), next_join_idx));
		step++;
	}

	join_orders.reserve(max_join_orders + 1);
	while (join_orders.size() <= max_join_orders && !queue.empty()) {
		auto entry = queue.top();
		auto &predecessors = entry.predecessors;
		queue.pop();

		predecessors.push_back(entry.candidate);
		auto join_candidates = FindJoinCandidates(hash_join_idxs.size(), predecessors, dependencies);

		if (predecessors.size() == hash_join_idxs.size() - 1 && join_candidates.size() == 1) {
			predecessors.push_back(join_candidates.front());
			join_orders.push_back(predecessors);
		} else {
			idx_t num_candidates = std::max(1, 4 - (int)predecessors.size()); // TODO: make constant
			num_candidates = std::min((idx_t)num_candidates, (idx_t)join_candidates.size());
			for (idx_t i = 0; i < num_candidates; i++) {
				idx_t candidate = selector->SelectNextCandidate(join_candidates, joins);
				join_candidates.erase(std::find(join_candidates.begin(), join_candidates.end(), candidate));
				queue.push(JoinCandidateEntry {predecessors.size(), i, step, predecessors, candidate});
				step++;
			}
		}
	}

	// TODO: Deduplicate this code in method
	bool contains_original_join_order = false;
	idx_t original_join_order_idx = 0;
	for (idx_t i = 0; i < join_orders.size(); i++) {
		auto &join_order = join_orders[i];
		bool is_original_join_order = true;
		for (idx_t j = 0; j < join_order.size(); j++) {
			if (join_order[j] != j) {
				is_original_join_order = false;
				break;
			}
		}
		if (is_original_join_order) {
			contains_original_join_order = true;
			original_join_order_idx = i;
			break;
		}
	}

	if (!contains_original_join_order) {
		vector<idx_t> original_join_order(hash_join_idxs.size());
		std::iota(original_join_order.begin(), original_join_order.end(), 0);
		join_orders.insert(join_orders.begin(), original_join_order);
		if (join_orders.size() > max_join_orders) {
			join_orders.erase(join_orders.end() - 1);
		}
	} else if (original_join_order_idx != 0) {
		auto original_join_order = join_orders[original_join_order_idx];
		join_orders.erase(join_orders.begin() + original_join_order_idx);
		join_orders.insert(join_orders.begin(), original_join_order);
	}
}

} // namespace duckdb
