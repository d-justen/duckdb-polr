#include "duckdb.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/parallel/thread_context.hpp"

#include <iostream>

using namespace duckdb;

int main() {
	DuckDB db(nullptr);
	Connection con(db);

	auto context = std::make_shared<ClientContext>(db.instance->shared_from_this());

	context->Query("CREATE TABLE table_a(a_a INTEGER, a_b INTEGER)", false);
	context->Query("CREATE TABLE table_b(b_a INTEGER, b_b INTEGER)", false);
	context->Query("CREATE TABLE table_c(c_a INTEGER, c_b INTEGER)", false);

	for (idx_t i = 0; i < 1000; i++) {
		context->Query("INSERT INTO table_a VALUES (" + std::to_string(i) + ", " + std::to_string(i) + ")", false);

		context->Query("INSERT INTO table_b VALUES (" + std::to_string(i) + ", " + std::to_string(i + 1000) + ")", false);
		context->Query("INSERT INTO table_b VALUES (" + std::to_string(i) + ", " + std::to_string(i + 2000) + ")", false);
		context->Query("INSERT INTO table_b VALUES (" + std::to_string(i) + ", " + std::to_string(i + 3000) + ")", false);

		context->Query("INSERT INTO table_c VALUES (" + std::to_string(i + 980) + ", " + std::to_string(i) + ")", false);
	}

	auto result = context->Query("SELECT * FROM table_a, table_b, table_c WHERE table_a.a_a = table_b.b_a AND table_a.a_a = table_c.c_a", false);
	result->Print();

	vector<LogicalType> types = {LogicalType(LogicalTypeId::INTEGER), LogicalType(LogicalTypeId::INTEGER)};
	auto &catalog = Catalog::GetCatalog(*context);
	auto table = (TableCatalogEntry *)
	    catalog.GetEntry(*context, CatalogType::TABLE_ENTRY, "main", "table_a");

	PhysicalTableScan scan(types, TableScanFunction::GetFunction(), make_unique<TableScanBindData>(table),
	                               {0, 1}, {"a, b"}, make_unique<TableFilterSet>(), 1000);

	//auto& executor = context->GetExecutor();
	//executor.Initialize(&scan);
	// executor->ExecuteTask();*/


}