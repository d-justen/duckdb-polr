//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// benchmark_runner.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "benchmark_configuration.hpp"
#include "benchmark.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/fstream.hpp"
#include <thread>

namespace duckdb {
class DuckDB;

//! The benchmark runner class is responsible for running benchmarks
class BenchmarkRunner {
	BenchmarkRunner();

public:
	static constexpr const char *DUCKDB_BENCHMARK_DIRECTORY = "duckdb_benchmark_data";
	BenchmarkConfiguration configuration;

	static BenchmarkRunner &GetInstance() {
		static BenchmarkRunner instance;
		return instance;
	}

	static void InitializeBenchmarkDirectory();

	//! Register a benchmark in the Benchmark Runner, this is done automatically
	//! as long as the proper macro's are used
	static void RegisterBenchmark(Benchmark *benchmark);

	void Log(string message);
	void LogLine(string message);
	void LogResult(string message);
	void LogOutput(string message);

	void RunBenchmark(Benchmark *benchmark);
	void RunBenchmarks();

	vector<Benchmark *> benchmarks;
	ofstream out_file;
	ofstream log_file;
	uint32_t threads = std::thread::hardware_concurrency();

	bool enable_polr = false;
	bool enable_polr_bushy = false;
	bool disable_cardinality_estimator = false;
	bool enable_random_cardinalities = false;
	bool mpx_alternate_chunks = false;
	double regret_budget = 0.2;
	bool log_tuples_routed = false;
	bool greedy_ordering = false;
	bool measure_pipeline = false;
	uint32_t nruns = 5;
};

} // namespace duckdb
