#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/constants.hpp"
#include "key_parser.hpp"
#include "level_pivot_storage.hpp"
#include <memory>
#include <unordered_map>

namespace duckdb {

// Virtual column IDs for identity/key columns used as row identifiers.
// These must be >= VIRTUAL_COLUMN_START (2^63). We use VIRTUAL_COLUMN_START + 1 + i
// to avoid colliding with COLUMN_IDENTIFIER_ROW_ID.
static constexpr column_t LEVEL_PIVOT_VIRTUAL_COL_BASE = 9223372036854775809ULL; // VIRTUAL_COLUMN_START + 1

enum class LevelPivotTableMode : uint8_t { PIVOT, RAW };

class LevelPivotCatalog;

class LevelPivotTableEntry : public TableCatalogEntry {
public:
	// Pivot mode constructor
	LevelPivotTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                     std::shared_ptr<level_pivot::LevelDBConnection> connection,
	                     std::unique_ptr<level_pivot::KeyParser> parser, vector<string> identity_columns,
	                     vector<string> attr_columns, vector<bool> column_json);

	// Raw mode constructor
	LevelPivotTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                     std::shared_ptr<level_pivot::LevelDBConnection> connection, vector<bool> column_json);

	LevelPivotTableMode GetTableMode() const {
		return mode_;
	}

	level_pivot::KeyParser &GetKeyParser() {
		return *parser_;
	}
	const level_pivot::KeyParser &GetKeyParser() const {
		return *parser_;
	}

	std::shared_ptr<level_pivot::LevelDBConnection> GetConnection() {
		return connection_;
	}

	const vector<string> &GetIdentityColumns() const {
		return identity_columns_;
	}
	const vector<string> &GetAttrColumns() const {
		return attr_columns_;
	}

	bool IsJsonColumn(idx_t col_idx) const {
		return col_idx < column_json_.size() && column_json_[col_idx];
	}

	// Map column name to its index in the column list
	idx_t GetColumnIndex(const string &name) const;

	// --- TableCatalogEntry interface ---
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
	vector<column_t> GetRowIdColumns() const override;
	virtual_column_map_t GetVirtualColumns() const override;

private:
	LevelPivotTableMode mode_;
	std::shared_ptr<level_pivot::LevelDBConnection> connection_;
	std::unique_ptr<level_pivot::KeyParser> parser_; // nullptr for raw mode
	vector<string> identity_columns_;
	vector<string> attr_columns_;
	vector<bool> column_json_;
	std::unordered_map<std::string, idx_t> col_name_to_index_;

	void BuildColumnIndexCache();
};

} // namespace duckdb
