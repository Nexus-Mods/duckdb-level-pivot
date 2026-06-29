#include "level_pivot_catalog.hpp"
#include "level_pivot_transaction.hpp"
#include "level_pivot_schema.hpp"
#include "level_pivot_table_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

struct DirtyTableRow {
	string database_name;
	string table_name;
	string table_mode;
};

struct DirtyTablesBindData : public TableFunctionData {
	vector<DirtyTableRow> rows;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> DirtyTablesBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<DirtyTablesBindData>();

	// Return columns
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("database_name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("table_name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("table_mode");

	// Enumerate all attached databases
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db_ref : databases) {
		auto &db = *db_ref;
		auto &catalog = db.GetCatalog();
		if (catalog.GetCatalogType() != "level_pivot") {
			continue;
		}

		// This context's transaction on this database (not a global "current" - there
		// can be several active across connections). This is the transaction that
		// staged the writes whose dirty set we report.
		auto &txn = Transaction::Get(context, catalog).Cast<LevelPivotTransaction>();
		if (!txn.HasDirtyTables()) {
			continue;
		}

		auto &lp_catalog = catalog.Cast<LevelPivotCatalog>();
		auto &schema = lp_catalog.GetMainSchema();
		auto &dirty = txn.GetDirtyTables();
		auto db_name = db.GetName();

		for (auto &table_name : dirty) {
			auto table_ptr = schema.GetTable(table_name);
			if (!table_ptr) {
				continue;
			}
			DirtyTableRow row;
			row.database_name = db_name;
			row.table_name = table_name;
			row.table_mode = table_ptr->GetTableMode() == LevelPivotTableMode::PIVOT ? "pivot" : "raw";
			data->rows.push_back(std::move(row));
		}
	}

	return std::move(data);
}

static void DirtyTablesFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<DirtyTablesBindData>();

	idx_t count = 0;
	while (bind_data.offset < bind_data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = bind_data.rows[bind_data.offset];
		output.SetValue(0, count, Value(row.database_name));
		output.SetValue(1, count, Value(row.table_name));
		output.SetValue(2, count, Value(row.table_mode));
		bind_data.offset++;
		count++;
	}
	output.SetCardinality(count);
}

TableFunction GetDirtyTablesFunction() {
	TableFunction func("level_pivot_dirty_tables", {}, DirtyTablesFunc, DirtyTablesBind);
	return func;
}

} // namespace duckdb
