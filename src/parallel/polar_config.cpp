#include "duckdb/parallel/polar_config.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"

#include <chrono>
#include <map>
#include <utility>

namespace duckdb {

POLARConfig::POLARConfig(duckdb::Pipeline *pipeline_p, unique_ptr<JoinEnumerationAlgo> enumerator_p)
    : pipeline(pipeline_p), enumerator(move(enumerator_p)),
      measure_polr_pipeline(pipeline_p->executor.context.config.measure_polr_pipeline),
      log_tuples_routed(pipeline_p->executor.context.config.log_tuples_routed) {
}

bool POLARConfig::GenerateJoinOrders() {
	D_ASSERT(hash_join_idxs.empty());
	const auto begin = std::chrono::system_clock::now();

	auto &pipeline_operators = pipeline->operators;
	auto &executor = pipeline->executor;

	if (pipeline_operators.empty()) {
		return false;
	}

	// Step I: Go through operators, save pointers to Joins (not only HJs?) in vector
	for (idx_t i = 0; i < pipeline_operators.size(); i++) {
		if (pipeline_operators[i]->type == PhysicalOperatorType::HASH_JOIN) {
			// We only want joins that directly follow each other
			if (hash_join_idxs.empty() || hash_join_idxs.back() == i - 1) {
				if (((PhysicalHashJoin *)pipeline_operators[i])->join_type == JoinType::INNER) {
					hash_join_idxs.push_back(i);
				}
			} else {
				break;
			}
		}
	}

	if (hash_join_idxs.size() <= 1) {
		return false;
	}

	vector<idx_t> num_columns_per_join;
	num_columns_per_join.reserve(hash_join_idxs.size());

	for (auto hash_join_idx : hash_join_idxs) {
		joins.push_back(static_cast<PhysicalHashJoin *>(pipeline_operators[hash_join_idx]));
		num_columns_per_join.push_back(joins.back()->types.size());
	}

	// Step II: If HJ-count > 1: Iterate through vector and build map of idx_t -> vector<idx_t>
	vector<idx_t> column_counts;
	column_counts.reserve(hash_join_idxs.size() + 1);
	column_counts.push_back(pipeline_operators[hash_join_idxs.front()]->children[0]->GetTypes().size());

	unordered_map<idx_t, vector<idx_t>> join_prerequisites;
	for (idx_t i = 0; i < hash_join_idxs.size(); i++) {
		join_prerequisites[i] = vector<idx_t>();
	}

	for (idx_t i = 0; i < hash_join_idxs.size(); i++) {
		auto join = (PhysicalHashJoin *)pipeline_operators[hash_join_idxs[i]];
		idx_t num_columns_from_right =
		    join->right_projection_map.empty() ? join->children[1]->types.size() : join->right_projection_map.size();
		column_counts.push_back(column_counts.back() + num_columns_from_right);

		for (idx_t j = 0; j < join->conditions.size(); j++) {
			auto &condition = join->conditions[j];
			auto *left_expr = &*condition.left;
			if (left_expr->type != ExpressionType::BOUND_REF) {
				if (left_expr->type != ExpressionType::CAST) {
					// Let's not POLAR, weird stuff going on
					return false;
				}
				auto &cast_expression = dynamic_cast<BoundCastExpression &>(*left_expr);
				left_expr = &*cast_expression.child;
			}

			auto &left_bound_expression = dynamic_cast<BoundReferenceExpression &>(*left_expr);

			if (left_bound_expression.index >= column_counts.front()) {
				for (idx_t k = 1; k < column_counts.size(); k++) {
					if (column_counts[k] > left_bound_expression.index) {
						join_prerequisites[i].push_back(k - 1);
						break;
					}
				}
			}
		}
	}

	enumerator->GenerateJoinOrders(hash_join_idxs, join_prerequisites, joins, join_paths);

	if (join_paths.size() < 2) {
		return false;
	}

	// Remove joins from operator vector
	// TODO: Make more efficient than copy, then erase...
	operators.insert(operators.end(), pipeline_operators.begin(), pipeline_operators.end());
	operators.erase(operators.begin() + hash_join_idxs.front(),
	                operators.begin() + hash_join_idxs.front() + hash_join_idxs.size());

	auto routing = DBConfig::GetConfig(executor.context).options.multiplexer_routing;
	auto prev_types = joins.front()->children[0]->GetTypes();
	adaptive_union = make_unique<PhysicalAdaptiveUnion>(
	    joins.back()->types, prev_types.size(), move(num_columns_per_join), joins.back()->estimated_cardinality);
	adaptive_union->op_state = adaptive_union->GetGlobalOperatorState(executor.context);

	double regret_budget = DBConfig::GetConfig(executor.context).options.regret_budget;
	if (DBConfig::GetConfig(executor.context).options.multiplexer_routing == MultiplexerRouting::EXPONENTIAL_BACKOFF) {
		idx_t max_threads = std::min((int32_t)pipeline->source_state->MaxThreads(),
		                             TaskScheduler::GetScheduler(executor.context).NumberOfThreads());
		regret_budget = pipeline->source->estimated_cardinality / 10240.0 / 10 / max_threads;
	}

	multiplexer = make_unique<PhysicalMultiplexer>(prev_types, joins.front()->children[0]->estimated_cardinality,
	                                               join_paths.size(), regret_budget, routing);
	multiplexer->op_state = multiplexer->GetGlobalOperatorState(executor.context);
	multiplexer_idx = hash_join_idxs.front();
	operators.insert(operators.begin() + multiplexer_idx, &*multiplexer);

	if (routing == MultiplexerRouting::BACKPRESSURE) {
		source_state = pipeline->source->GetGlobalSourceState(pipeline->executor.context);
		backpressure_pipelines = make_unique<vector<unique_ptr<Pipeline>>>();
		backpressure_pipelines->reserve(join_paths.size());

		for (idx_t i = 0; i < join_paths.size(); i++) {
			backpressure_pipelines->emplace_back(make_unique<Pipeline>(executor));
			auto &backpressure_pipeline = *backpressure_pipelines->back();
			backpressure_pipeline.source = pipeline->source;
			backpressure_pipeline.sink = pipeline->sink;
			backpressure_pipeline.parents = pipeline->parents;
			backpressure_pipeline.dependencies = pipeline->dependencies;
			backpressure_pipeline.is_backpressure_pipeline = true;
			backpressure_pipeline.initialized = true;
			backpressure_pipeline.ready = true;
			backpressure_pipeline.operators = pipeline_operators;
			const auto &join_order = join_paths[i];
			backpressure_pipeline.backpressure_join_order = make_unique<vector<idx_t>>(join_order);
		}
	}

	// Depending on the join order, the join conditions may have to use different columns idxs for probing.
	// Here we build a map that the pipeline executor can use to build JoinOperatorStates containing
	// expressions for the different probe column idxs.
	left_expression_bindings.reserve(join_paths.size());

	const idx_t seed_table_column_count = prev_types.size();
	vector<idx_t> column_offsets;
	column_offsets.reserve(joins.size() + 1);
	column_offsets.push_back(seed_table_column_count);

	// join_idx, join_condition_idx, probe_side_join_idx, relative_column_idx
	std::map<idx_t, std::map<idx_t, pair<idx_t, idx_t>>> relative_column_binding_map;

	for (idx_t i = 0; i < joins.size(); i++) {
		auto &join = joins[i];
		idx_t num_columns_from_right =
		    join->right_projection_map.empty() ? join->children[1]->types.size() : join->right_projection_map.size();
		column_offsets.push_back(column_offsets.back() + num_columns_from_right);

		for (idx_t j = 0; j < join->conditions.size(); j++) {
			auto &condition = join->conditions[j];
			auto *left_expr = &*condition.left;
			if (left_expr->type != ExpressionType::BOUND_REF) {
				auto &cast_expression = dynamic_cast<BoundCastExpression &>(*left_expr);
				left_expr = &*cast_expression.child;
			}

			auto &left_bound_expression = dynamic_cast<BoundReferenceExpression &>(*left_expr);

			if (left_bound_expression.index >= seed_table_column_count) {
				for (idx_t k = 1; k < column_offsets.size(); k++) {
					if (column_offsets[k] > left_bound_expression.index) {
						idx_t join_idx = k - 1;
						idx_t relative_column_idx = left_bound_expression.index - column_offsets[join_idx];
						relative_column_binding_map[i][j] = make_pair(join_idx, relative_column_idx);
						break;
					}
				}
			}
		}
	}

	for (idx_t join_path_idx = 0; join_path_idx < join_paths.size(); join_path_idx++) {
		auto &join_path = join_paths[join_path_idx];
		vector<std::map<idx_t, idx_t>> expression_bindings(join_path.size());
		vector<idx_t> current_join_path_column_offsets;

		current_join_path_column_offsets.reserve(join_path.size() + 1);
		current_join_path_column_offsets.push_back(seed_table_column_count);

		for (idx_t j = 0; j < join_path.size(); j++) {
			auto join_idx = join_path[j];
			auto column_bindings = relative_column_binding_map.find(join_idx);
			if (column_bindings != relative_column_binding_map.cend()) {
				auto &binding_map = column_bindings->second;

				for (auto &binding : binding_map) {
					auto probe_join_idx = binding.second.first;
					auto relative_column_idx = binding.second.second;

					for (idx_t i = 0; i < current_join_path_column_offsets.size(); i++) {
						if (join_path[i] == probe_join_idx) {
							idx_t probe_column_idx = current_join_path_column_offsets[i] + relative_column_idx;
							expression_bindings[j][binding.first] = probe_column_idx;
						}
					}
				}
			}

			idx_t additional_columns = joins[join_idx]->right_projection_map.empty()
			                               ? joins[join_idx]->children[1]->types.size()
			                               : joins[join_idx]->right_projection_map.size();

			current_join_path_column_offsets.push_back(current_join_path_column_offsets.back() + additional_columns);
		}

		left_expression_bindings.push_back(expression_bindings);
		if (routing == MultiplexerRouting::BACKPRESSURE) {
			backpressure_pipelines->at(join_path_idx)->polar_bindings = expression_bindings;
		}
	}

	if (pipeline->executor.context.config.log_tuples_routed) {
		const auto end = std::chrono::system_clock::now();

		std::string filename = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
		std::ofstream file;
		string &prefix = DBConfig::GetConfig(executor.context).options.dir_prefix;
		char tmp[256];
		getcwd(tmp, 256);

		file.open(std::string(tmp) + "/tmp/" + prefix + filename + "-enumeration.csv");

		double duration_ms = std::chrono::duration<double, std::milli>(end - begin).count();
		file << "num_joins,enumeration_time_ms\n";
		file << hash_join_idxs.size() << "," << duration_ms << "\n";
		file.close();
	}

	return true;
}

} // namespace duckdb
