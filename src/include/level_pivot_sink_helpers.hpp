#pragma once

#include "level_pivot_table_entry.hpp"
#include "level_pivot_catalog.hpp"
#include "level_pivot_transaction.hpp"
#include "level_pivot_storage.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

struct LevelPivotSinkGlobalState : public GlobalSinkState {
	idx_t row_count = 0;
};

struct SinkContext {
	LevelPivotTableEntry &table;
	level_pivot::LevelDBConnection &connection;
	LevelPivotTransaction &txn;
	LevelPivotSchemaEntry &schema;
};

inline SinkContext GetSinkContext(ExecutionContext &context, TableCatalogEntry &table_ref) {
	auto &lp_table = table_ref.Cast<LevelPivotTableEntry>();
	auto &connection = *lp_table.GetConnection();
	auto &catalog = lp_table.ParentCatalog().Cast<LevelPivotCatalog>();
	auto &txn = Transaction::Get(context.client, catalog).Cast<LevelPivotTransaction>();
	auto &schema = catalog.GetMainSchema();
	return {lp_table, connection, txn, schema};
}

struct SinkBatch {
	LevelPivotTransaction &txn;

	void put(std::string_view key, std::string_view value) {
		txn.StagePut(key, value);
	}
	void del(std::string_view key) {
		txn.StageDelete(key);
	}
};

inline SinkBatch GetSinkBatch(const SinkContext &ctx) {
	return SinkBatch {ctx.txn};
}

inline SourceResultType EmitRowCount(GlobalSinkState &sink_state, DataChunk &chunk) {
	auto &gstate = sink_state.Cast<LevelPivotSinkGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(static_cast<int64_t>(gstate.row_count)));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
