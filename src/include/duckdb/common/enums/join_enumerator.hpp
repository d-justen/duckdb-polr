//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/join_enumerator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class JoinEnumerator : uint8_t {
	DFS_RANDOM,
	DFS_MIN_CARD,
	DFS_UNCERTAIN,
	BFS_RANDOM,
	BFS_MIN_CARD,
	BFS_UNCERTAIN,
	EACH_LAST_ONCE,
	EACH_FIRST_ONCE,
	SAMPLE
};

} // namespace duckdb
