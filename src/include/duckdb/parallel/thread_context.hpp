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
	idx_t current_path_idx;
	//! The number of tuples in that path
	idx_t current_path_input_tuple_count;
	//! Function for adaptive union to call to mark tuple arrival
	std::function<void()> update_path_weight;
};

} // namespace duckdb
