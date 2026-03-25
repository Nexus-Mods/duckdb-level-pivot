#include "level_pivot_update.hpp"
#include "key_parser.hpp"
#include "level_pivot_sink_helpers.hpp"
#include "level_pivot_utils.hpp"

namespace duckdb {

LevelPivotUpdate::LevelPivotUpdate(PhysicalPlan &plan, vector<LogicalType> types, TableCatalogEntry &table,
                                   vector<PhysicalIndex> columns, idx_t estimated_cardinality)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality), table(table),
      columns(std::move(columns)) {
}

unique_ptr<GlobalSinkState> LevelPivotUpdate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<LevelPivotSinkGlobalState>();
}

SinkResultType LevelPivotUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<LevelPivotSinkGlobalState>();
	auto ctx = GetSinkContext(context, table);

	if (ctx.table.GetTableMode() == LevelPivotTableMode::PIVOT) {
		auto &parser = ctx.table.GetKeyParser();
		auto &table_columns = ctx.table.GetColumns();
		auto &identity_cols = ctx.table.GetIdentityColumns();
		auto row_id_cols = ctx.table.GetRowIdColumns();
		auto batch = ctx.connection.create_batch();

		// The child chunk layout (from DuckDB's update projection):
		// [update_col_0, update_col_1, ..., row_id_col_0, row_id_col_1, ...]
		// Row ID columns are at the END of the chunk
		idx_t num_update_cols = this->columns.size();
		idx_t num_row_id_cols = row_id_cols.size();
		idx_t row_id_offset = chunk.ColumnCount() - num_row_id_cols;

		std::vector<std::string> identity_values;
		identity_values.reserve(num_row_id_cols);

		for (idx_t row = 0; row < chunk.size(); row++) {
			// Extract identity from row_id columns (at end of chunk)
			ExtractIdentityValues(identity_values, chunk, row, row_id_offset, num_row_id_cols);

			// Process each updated column (at beginning of chunk)
			for (idx_t i = 0; i < num_update_cols; i++) {
				auto physical_idx = this->columns[i].index;
				auto &col = table_columns.GetColumn(PhysicalIndex(physical_idx));
				auto &col_name = col.Name();

				// Get the new value from the chunk
				auto new_val = chunk.data[i].GetValue(row);

				// Check if this is an identity column
				bool is_identity = false;
				for (auto &id_col : identity_cols) {
					if (id_col == col_name) {
						is_identity = true;
						break;
					}
				}

				if (is_identity) {
					throw NotImplementedException("Updating identity columns is not yet supported");
				}

				// It's an attr column - update the specific key
				std::string key = parser.build(identity_values, col_name);
				if (new_val.IsNull()) {
					batch.del(key);
				} else {
					auto table_col_idx = ctx.table.GetColumnIndex(col_name);
					if (ctx.table.IsJsonColumn(table_col_idx)) {
						batch.put(key, TypedValueToJsonString(new_val, col.Type()));
					} else {
						batch.put(key, new_val.ToString());
					}
				}
				ctx.txn.CheckKeyAgainstTables(key, ctx.schema);
			}
		}

		batch.commit();
		gstate.row_count += chunk.size();
	} else {
		// Raw mode: chunk layout is [update_value, row_id_key]
		bool val_is_json = ctx.table.IsJsonColumn(1);
		auto &val_col_type = ctx.table.GetColumns().GetColumn(LogicalIndex(1)).Type();
		auto batch = ctx.connection.create_batch();
		idx_t key_col_idx = chunk.ColumnCount() - 1;
		for (idx_t row = 0; row < chunk.size(); row++) {
			auto key_val = chunk.data[key_col_idx].GetValue(row);
			if (key_val.IsNull()) {
				continue;
			}
			auto val = chunk.data[0].GetValue(row);
			std::string key = key_val.ToString();
			if (val.IsNull()) {
				batch.put(key, "");
			} else if (val_is_json) {
				batch.put(key, TypedValueToJsonString(val, val_col_type));
			} else {
				batch.put(key, val.ToString());
			}
			ctx.txn.CheckKeyAgainstTables(key, ctx.schema);
		}
		batch.commit();
		gstate.row_count += chunk.size();
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType LevelPivotUpdate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	return SinkFinalizeType::READY;
}

SourceResultType LevelPivotUpdate::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	return EmitRowCount(*sink_state, chunk);
}

} // namespace duckdb
