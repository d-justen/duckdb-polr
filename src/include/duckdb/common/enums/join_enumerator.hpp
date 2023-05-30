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

enum class JoinEnumerator : uint8_t { EXHAUSTIVE, EACH_LAST_ONCE };

} // namespace duckdb
