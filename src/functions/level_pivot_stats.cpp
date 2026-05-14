#include "level_pivot_stats.hpp"
#include "level_pivot_catalog.hpp"
#include "level_pivot_storage.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct StatsRow {
	string database_name;
	uint64_t total_writes;
	uint64_t total_puts;
	uint64_t total_deletes;
};

struct StatsBindData : public TableFunctionData {
	vector<StatsRow> rows;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> StatsBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<StatsBindData>();

	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("database_name");
	return_types.push_back(LogicalType::UBIGINT);
	names.push_back("total_writes");
	return_types.push_back(LogicalType::UBIGINT);
	names.push_back("total_puts");
	return_types.push_back(LogicalType::UBIGINT);
	names.push_back("total_deletes");

	bool has_filter = !input.inputs.empty() && !input.inputs[0].IsNull();
	string filter_db = has_filter ? input.inputs[0].GetValue<string>() : "";

	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db_ref : databases) {
		auto &db = *db_ref;
		auto &catalog = db.GetCatalog();
		if (catalog.GetCatalogType() != "level_pivot") {
			continue;
		}
		if (has_filter && db.GetName() != filter_db) {
			continue;
		}

		auto &lp_catalog = catalog.Cast<LevelPivotCatalog>();
		auto conn = lp_catalog.GetConnection();
		StatsRow r;
		r.database_name = db.GetName();
		r.total_writes = conn->total_writes();
		r.total_puts = conn->total_puts();
		r.total_deletes = conn->total_deletes();
		data->rows.push_back(std::move(r));
	}

	return std::move(data);
}

static void StatsFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<StatsBindData>();

	idx_t count = 0;
	while (bind_data.offset < bind_data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = bind_data.rows[bind_data.offset];
		output.SetValue(0, count, Value(row.database_name));
		output.SetValue(1, count, Value::UBIGINT(row.total_writes));
		output.SetValue(2, count, Value::UBIGINT(row.total_puts));
		output.SetValue(3, count, Value::UBIGINT(row.total_deletes));
		bind_data.offset++;
		count++;
	}
	output.SetCardinality(count);
}

TableFunctionSet GetStatsFunctionSet() {
	TableFunctionSet set("level_pivot_stats");
	set.AddFunction(TableFunction("level_pivot_stats", {}, StatsFunc, StatsBind));
	set.AddFunction(TableFunction("level_pivot_stats", {LogicalType::VARCHAR}, StatsFunc, StatsBind));
	return set;
}

} // namespace duckdb
