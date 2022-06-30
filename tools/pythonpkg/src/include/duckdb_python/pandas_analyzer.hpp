//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pandas_analyzer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb_python/python_object_container.hpp"
#include "duckdb_python/pandas_type.hpp"
#include "duckdb_python/python_conversion.hpp"

namespace duckdb {

class PandasAnalyzer {
public:
	PandasAnalyzer(const DBConfig &config) {
		analyzed_type = LogicalType::SQLNULL;

		auto percentage_entry = config.set_variables.find("analyze_sample_percentage");
		D_ASSERT(percentage_entry != config.set_variables.end());
		sample_percentage = percentage_entry->second.GetValue<double>();

		auto minimum_entry = config.set_variables.find("analyze_sample_minimum");
		D_ASSERT(minimum_entry != config.set_variables.end());
		sample_minimum = minimum_entry->second.GetValue<uint64_t>();
	}

public:
	LogicalType GetListType(py::handle &ele, bool &can_convert);
	LogicalType DictToMap(const PyDictionary &dict, bool &can_convert);
	LogicalType DictToStruct(const PyDictionary &dict, bool &can_convert);
	LogicalType GetItemType(py::handle &ele, bool &can_convert);
	bool Analyze(py::handle column);
	LogicalType AnalyzedType() {
		return analyzed_type;
	}

private:
	LogicalType InnerAnalyze(py::handle column, bool &can_convert, bool sample = true, idx_t increment = 1);
	uint64_t GetSampleIncrement(idx_t rows);

private:
	double sample_percentage;
	uint64_t sample_minimum;
	//! Holds the gil to allow python object creation/destruction
	PythonGILWrapper gil;
	//! The resulting analyzed type
	LogicalType analyzed_type;
};

} // namespace duckdb
