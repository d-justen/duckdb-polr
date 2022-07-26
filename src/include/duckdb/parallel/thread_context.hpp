//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/thread_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/query_profiler.hpp"

#include <functional>

namespace duckdb {
class ClientContext;

//! The ThreadContext holds thread-local info for parallel usage
class ThreadContext {
public:
	explicit ThreadContext(ClientContext &context);

	//! The operator profiler for the individual thread context
	OperatorProfiler profiler;
	//! The next POLR path to choose
	vector<idx_t> *current_join_path;
	vector<vector<LogicalType>> join_build_types;
};

} // namespace duckdb
