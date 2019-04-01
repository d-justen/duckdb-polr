//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/expression_binder/insert_binder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/expression_binder.hpp"

namespace duckdb {

//! The INSERT binder is responsible for binding expressions within the VALUES of an INSERT statement
class InsertBinder : public ExpressionBinder {
public:
	InsertBinder(Binder &binder, ClientContext &context);

protected:
	BindResult BindExpression(ParsedExpression &expr, uint32_t depth, bool root_expression = false) override;
};

} // namespace duckdb
