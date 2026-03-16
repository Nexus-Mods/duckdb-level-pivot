#include "level_pivot_catalog.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types.hpp"
#include <algorithm>

namespace duckdb {

struct CreateTableBindData : public TableFunctionData {
	string catalog_name;
	string table_name;
	string pattern;
	vector<string> column_names;
	vector<LogicalType> column_types;
	vector<bool> column_json;
	string table_mode; // "pivot" or "raw"
	bool done = false;
};

static unique_ptr<FunctionData> CreateTableBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<CreateTableBindData>();

	// Arguments: catalog_name, table_name, pattern, column_names
	data->catalog_name = input.inputs[0].GetValue<string>();
	data->table_name = input.inputs[1].GetValue<string>();

	auto pattern_val = input.inputs[2];
	if (pattern_val.IsNull()) {
		data->pattern = "";
	} else {
		data->pattern = pattern_val.GetValue<string>();
	}

	// Column names from list
	auto &col_list = ListValue::GetChildren(input.inputs[3]);
	for (auto &col : col_list) {
		data->column_names.push_back(col.GetValue<string>());
	}

	// Check for column_types named parameter
	auto ct_it = input.named_parameters.find("column_types");
	if (ct_it != input.named_parameters.end()) {
		auto &type_list = ListValue::GetChildren(ct_it->second);
		if (type_list.size() != data->column_names.size()) {
			throw InvalidInputException("column_types length (%d) must match column_names length (%d)",
			                            type_list.size(), data->column_names.size());
		}
		for (auto &type_val : type_list) {
			auto type_str = type_val.GetValue<string>();
			// Check for case-insensitive "JSON " prefix
			bool is_json = false;
			if (type_str.size() >= 5) {
				auto prefix = type_str.substr(0, 5);
				std::transform(prefix.begin(), prefix.end(), prefix.begin(), ::toupper);
				if (prefix == "JSON ") {
					is_json = true;
					type_str = type_str.substr(5);
				}
			}
			data->column_json.push_back(is_json);
			data->column_types.push_back(TransformStringToLogicalType(type_str, context));
		}
	} else {
		// Default: all VARCHAR, no JSON
		data->column_types.resize(data->column_names.size(), LogicalType::VARCHAR);
		data->column_json.resize(data->column_names.size(), false);
	}

	// Check for table_mode named parameter
	data->table_mode = "pivot";
	auto it = input.named_parameters.find("table_mode");
	if (it != input.named_parameters.end()) {
		data->table_mode = it->second.GetValue<string>();
	}

	if (data->table_mode != "pivot" && data->table_mode != "raw") {
		throw InvalidInputException("Invalid table_mode '%s'. Must be 'pivot' or 'raw'.", data->table_mode);
	}

	// Return type: single boolean column
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("success");

	return std::move(data);
}

static void CreateTableFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<CreateTableBindData>();

	if (bind_data.done) {
		output.SetCardinality(0);
		return;
	}

	auto &catalog = Catalog::GetCatalog(context, bind_data.catalog_name);
	auto &lp_catalog = catalog.Cast<LevelPivotCatalog>();

	if (bind_data.table_mode == "raw") {
		lp_catalog.CreateRawTable(bind_data.table_name, bind_data.column_names, bind_data.column_types,
		                          bind_data.column_json);
	} else {
		if (bind_data.pattern.empty()) {
			throw InvalidInputException("Pattern is required for pivot tables");
		}
		lp_catalog.CreatePivotTable(bind_data.table_name, bind_data.pattern, bind_data.column_names,
		                            bind_data.column_types, bind_data.column_json);
	}

	output.SetCardinality(1);
	output.data[0].SetValue(0, Value::BOOLEAN(true));
	bind_data.done = true;
}

TableFunction GetCreateTableFunction() {
	TableFunction func(
	    "level_pivot_create_table",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::VARCHAR)},
	    CreateTableFunc, CreateTableBind);
	func.named_parameters["table_mode"] = LogicalType::VARCHAR;
	func.named_parameters["column_types"] = LogicalType::LIST(LogicalType::VARCHAR);
	return func;
}

TableFunction GetDropTableFunction() {
	auto drop_bind = [](ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
	                    vector<string> &names) -> unique_ptr<FunctionData> {
		auto data = make_uniq<CreateTableBindData>();
		data->catalog_name = input.inputs[0].GetValue<string>();
		data->table_name = input.inputs[1].GetValue<string>();
		return_types.push_back(LogicalType::BOOLEAN);
		names.push_back("success");
		return std::move(data);
	};

	auto drop_func = [](ClientContext &context, TableFunctionInput &data, DataChunk &output) {
		auto &bind_data = data.bind_data->CastNoConst<CreateTableBindData>();
		if (bind_data.done) {
			output.SetCardinality(0);
			return;
		}
		auto &catalog = Catalog::GetCatalog(context, bind_data.catalog_name);
		auto &lp_catalog = catalog.Cast<LevelPivotCatalog>();
		lp_catalog.DropTable(bind_data.table_name);
		output.SetCardinality(1);
		output.data[0].SetValue(0, Value::BOOLEAN(true));
		bind_data.done = true;
	};

	TableFunction func("level_pivot_drop_table", {LogicalType::VARCHAR, LogicalType::VARCHAR}, drop_func, drop_bind);
	return func;
}

} // namespace duckdb
