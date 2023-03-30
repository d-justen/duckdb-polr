//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb_benchmark.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "benchmark.hpp"
#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

//! Base class for any state that has to be kept by a Benchmark
struct DuckDBBenchmarkState : public BenchmarkState {
	DuckDB db;
	Connection conn;
	unique_ptr<QueryResult> result;

	DuckDBBenchmarkState(string path) : db(path.empty() ? nullptr : path.c_str()), conn(db) {
		auto &instance = BenchmarkRunner::GetInstance();
		auto res = conn.Query("PRAGMA threads=" + to_string(instance.threads));
		D_ASSERT(!res->HasError());

		auto res_regret_budget = conn.Query("SET regret_budget TO " + to_string(instance.regret_budget));
		D_ASSERT(!res->HasError());

		string profiling_mode;
		switch (instance.configuration.profile_info) {
		case BenchmarkProfileInfo::NONE:
			profiling_mode = "";
			break;
		case BenchmarkProfileInfo::NORMAL:
			profiling_mode = "standard";
			break;
		case BenchmarkProfileInfo::DETAILED:
			profiling_mode = "detailed";
			break;
		default:
			throw InternalException("Unknown profiling option \"%s\"", instance.configuration.profile_info);
		}
		if (!profiling_mode.empty()) {
			res = conn.Query("PRAGMA profiling_mode=" + profiling_mode);
			D_ASSERT(!res->HasError());
		}
		if (instance.enable_polr) {
			res = conn.Query("PRAGMA enable_polr");
			D_ASSERT(!res->HasError());
		}
		if (instance.enable_polr_bushy) {
			res = conn.Query("PRAGMA enable_polr_bushy");
			D_ASSERT(!res->HasError());
		}
		if (instance.disable_cardinality_estimator) {
			res = conn.Query("PRAGMA disable_cardinality_estimator");
			D_ASSERT(!res->HasError());
		}
		if (instance.enable_random_cardinalities) {
			res = conn.Query("PRAGMA enable_random_cardinalities");
			D_ASSERT(!res->HasError());
		}
		if (instance.mpx_alternate_chunks) {
			res = conn.Query("PRAGMA enable_mpx_alternate_chunks");
			D_ASSERT(!res->HasError());
		}
		if (instance.log_tuples_routed) {
			res = conn.Query("PRAGMA enable_log_tuples_routed");
			D_ASSERT(!res->HasError());
		}
		if (instance.greedy_ordering) {
			res = conn.Query("PRAGMA enable_greedy_ordering");
			D_ASSERT(!res->HasError());
		}
		if (instance.measure_pipeline) {
			res = conn.Query("PRAGMA enable_measure_pipeline");
			D_ASSERT(!res->HasError());
		}
	}
	virtual ~DuckDBBenchmarkState() {
	}
};

//! The base Benchmark class is a base class that is used to create and register
//! new benchmarks
class DuckDBBenchmark : public Benchmark {
public:
	DuckDBBenchmark(bool register_benchmark, string name, string group) : Benchmark(register_benchmark, name, group) {
	}
	virtual ~DuckDBBenchmark() {
	}

	//! Load data into DuckDB
	virtual void Load(DuckDBBenchmarkState *state) = 0;
	//! Run a bunch of queries, only called if GetQuery() returns an empty string
	virtual void RunBenchmark(DuckDBBenchmarkState *state) {
	}
	//! This function gets called after the GetQuery() method
	virtual void Cleanup(DuckDBBenchmarkState *state) {};
	//! Verify a result
	virtual string VerifyResult(QueryResult *result) = 0;
	//! Whether or not the benchmark is performed on an in-memory database
	virtual bool InMemory() {
		return true;
	}

	string GetDatabasePath() {
		if (!InMemory()) {
			string path = "duckdb_benchmark_db.db";
			DeleteDatabase(path);
			return path;
		} else {
			return string();
		}
	}

	virtual unique_ptr<DuckDBBenchmarkState> CreateBenchmarkState() {
		return make_unique<DuckDBBenchmarkState>(GetDatabasePath());
	}

	unique_ptr<BenchmarkState> Initialize(BenchmarkConfiguration &config) override {
		auto state = CreateBenchmarkState();
		Load(state.get());
		return std::move(state);
	}

	void Run(BenchmarkState *state_p) override {
		auto state = (DuckDBBenchmarkState *)state_p;
		string query = GetQuery();
		if (query.empty()) {
			RunBenchmark(state);
		} else {
			state->result = state->conn.Query(query);
		}
	}

	void Cleanup(BenchmarkState *state_p) override {
		auto state = (DuckDBBenchmarkState *)state_p;
		Cleanup(state);
	}

	string Verify(BenchmarkState *state_p) override {
		auto state = (DuckDBBenchmarkState *)state_p;
		return VerifyResult(state->result.get());
	}

	string GetLogOutput(BenchmarkState *state_p) override {
		auto state = (DuckDBBenchmarkState *)state_p;
		auto &profiler = QueryProfiler::Get(*state->conn.context);
		return profiler.ToJSON();
	}

	//! Interrupt the benchmark because of a timeout
	void Interrupt(BenchmarkState *state_p) override {
		auto state = (DuckDBBenchmarkState *)state_p;
		state->conn.Interrupt();
	}
};

} // namespace duckdb
