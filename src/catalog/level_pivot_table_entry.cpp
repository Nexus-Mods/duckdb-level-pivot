#include "level_pivot_table_entry.hpp"
#include "level_pivot_scan.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/common/table_column.hpp"

namespace duckdb {

// Pivot mode constructor
LevelPivotTableEntry::LevelPivotTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                           std::shared_ptr<level_pivot::LevelDBConnection> connection,
                                           std::unique_ptr<level_pivot::KeyParser> parser,
                                           vector<string> identity_columns, vector<string> attr_columns,
                                           vector<bool> column_json)
    : TableCatalogEntry(catalog, schema, info), mode_(LevelPivotTableMode::PIVOT), connection_(std::move(connection)),
      parser_(std::move(parser)), identity_columns_(std::move(identity_columns)),
      attr_columns_(std::move(attr_columns)), column_json_(std::move(column_json)) {
	BuildColumnIndexCache();
}

// Raw mode constructor
LevelPivotTableEntry::LevelPivotTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                           std::shared_ptr<level_pivot::LevelDBConnection> connection,
                                           vector<bool> column_json)
    : TableCatalogEntry(catalog, schema, info), mode_(LevelPivotTableMode::RAW), connection_(std::move(connection)),
      column_json_(std::move(column_json)) {
	// For raw mode: column 0 = key, column 1 = value
	if (info.columns.LogicalColumnCount() >= 1) {
		identity_columns_.push_back(info.columns.GetColumn(LogicalIndex(0)).Name());
	}
	BuildColumnIndexCache();
}

void LevelPivotTableEntry::BuildColumnIndexCache() {
	for (auto &col : GetColumns().Logical()) {
		col_name_to_index_[col.Name()] = col.Logical().index;
	}
}

idx_t LevelPivotTableEntry::GetColumnIndex(const string &col_name) const {
	auto it = col_name_to_index_.find(col_name);
	if (it != col_name_to_index_.end()) {
		return it->second;
	}
	throw InternalException("Column '%s' not found in table '%s'", col_name, name);
}

TableFunction LevelPivotTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto data = make_uniq<LevelPivotScanData>();
	data->table_entry = this;
	bind_data = std::move(data);
	return LevelPivotScanFunction();
}

unique_ptr<BaseStatistics> LevelPivotTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

TableStorageInfo LevelPivotTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	result.cardinality = 0;
	return result;
}

vector<column_t> LevelPivotTableEntry::GetRowIdColumns() const {
	vector<column_t> result;
	if (mode_ == LevelPivotTableMode::PIVOT) {
		for (idx_t i = 0; i < identity_columns_.size(); i++) {
			result.push_back(LEVEL_PIVOT_VIRTUAL_COL_BASE + i);
		}
	} else {
		result.push_back(LEVEL_PIVOT_VIRTUAL_COL_BASE);
	}
	return result;
}

virtual_column_map_t LevelPivotTableEntry::GetVirtualColumns() const {
	virtual_column_map_t result;
	result.insert(make_pair(COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", LogicalType::ROW_TYPE)));
	if (mode_ == LevelPivotTableMode::PIVOT) {
		for (idx_t i = 0; i < identity_columns_.size(); i++) {
			auto it = col_name_to_index_.find(identity_columns_[i]);
			if (it != col_name_to_index_.end()) {
				auto &col = GetColumns().GetColumn(LogicalIndex(it->second));
				result.insert(make_pair(LEVEL_PIVOT_VIRTUAL_COL_BASE + i, TableColumn(col.Name(), col.Type())));
			}
		}
	} else {
		auto &key_col = GetColumns().GetColumn(LogicalIndex(0));
		result.insert(make_pair(LEVEL_PIVOT_VIRTUAL_COL_BASE, TableColumn(key_col.Name(), key_col.Type())));
	}
	return result;
}

} // namespace duckdb
