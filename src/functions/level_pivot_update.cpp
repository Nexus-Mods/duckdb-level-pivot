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
	ctx.txn.MarkTableDirty(ctx.table.name);

	if (ctx.table.GetTableMode() == LevelPivotTableMode::PIVOT) {
		auto &parser = ctx.table.GetKeyParser();
		auto &table_columns = ctx.table.GetColumns();
		auto &identity_cols = ctx.table.GetIdentityColumns();
		auto row_id_cols = ctx.table.GetRowIdColumns();
		auto batch = GetSinkBatch(ctx);

		// The child chunk layout (from DuckDB's update projection):
		// [update_col_0, update_col_1, ..., row_id_col_0, row_id_col_1, ...]
		// Row ID columns are at the END of the chunk
		idx_t num_update_cols = this->columns.size();
		idx_t num_row_id_cols = row_id_cols.size();
		idx_t row_id_offset = chunk.ColumnCount() - num_row_id_cols;

		// Pre-resolve metadata for each update column and pre-build UVF where applicable
		struct UpdateColMeta {
			std::string col_name;
			bool is_identity;
			bool is_json;
			LogicalType col_type;
			bool fast_path = false; // true → plain VARCHAR, use UVF
			UnifiedVectorFormat fmt;
			const string_t *data = nullptr;
		};
		std::vector<UpdateColMeta> update_meta;
		update_meta.reserve(num_update_cols);
		for (idx_t i = 0; i < num_update_cols; i++) {
			auto physical_idx = this->columns[i].index;
			auto &col = table_columns.GetColumn(PhysicalIndex(physical_idx));
			UpdateColMeta meta;
			meta.col_name = col.Name();
			meta.is_identity = false;
			for (auto &id_col : identity_cols) {
				if (id_col == meta.col_name) {
					meta.is_identity = true;
					break;
				}
			}
			auto table_col_idx = ctx.table.GetColumnIndex(meta.col_name);
			meta.is_json = ctx.table.IsJsonColumn(table_col_idx);
			meta.col_type = col.Type();
			bool is_varchar = meta.col_type.id() == LogicalTypeId::VARCHAR;
			if (is_varchar && !meta.is_json && !meta.is_identity) {
				meta.fast_path = true;
				chunk.data[i].ToUnifiedFormat(chunk.size(), meta.fmt);
				meta.data = UnifiedVectorFormat::GetData<string_t>(meta.fmt);
			}
			update_meta.push_back(std::move(meta));
		}

		std::vector<std::string> identity_values;
		identity_values.reserve(num_row_id_cols);

		for (idx_t row = 0; row < chunk.size(); row++) {
			// Extract identity from row_id columns (at end of chunk)
			ExtractIdentityValues(identity_values, chunk, row, row_id_offset, num_row_id_cols);

			// Process each updated column (at beginning of chunk)
			for (idx_t i = 0; i < num_update_cols; i++) {
				auto &meta = update_meta[i];

				if (meta.is_identity) {
					throw NotImplementedException("Updating identity columns is not yet supported");
				}

				// It's an attr column - update the specific key
				std::string key = parser.build(identity_values, meta.col_name);

				if (meta.fast_path) {
					auto vidx = meta.fmt.sel->get_index(row);
					if (!meta.fmt.validity.RowIsValid(vidx)) {
						batch.del(key);
					} else {
						batch.put(key, std::string_view(meta.data[vidx].GetData(), meta.data[vidx].GetSize()));
					}
				} else {
					// Slow path: typed or JSON columns go through Value
					auto new_val = chunk.data[i].GetValue(row);
					if (new_val.IsNull()) {
						batch.del(key);
					} else if (meta.is_json) {
						batch.put(key, TypedValueToJsonString(new_val, meta.col_type));
					} else {
						batch.put(key, new_val.ToString());
					}
				}
				ctx.txn.CheckKeyAgainstTables(key, ctx.schema);
			}
		}

		gstate.row_count += chunk.size();
	} else {
		// Raw mode: chunk layout is [update_value, row_id_key]
		auto &key_col_type = ctx.table.GetColumns().GetColumn(LogicalIndex(0)).Type();
		auto &val_col_type = ctx.table.GetColumns().GetColumn(LogicalIndex(1)).Type();
		bool key_is_varchar = key_col_type.id() == LogicalTypeId::VARCHAR;
		bool val_is_varchar = val_col_type.id() == LogicalTypeId::VARCHAR;
		bool val_is_json = ctx.table.IsJsonColumn(1);
		auto batch = GetSinkBatch(ctx);
		idx_t key_col_idx = chunk.ColumnCount() - 1;

		if (key_is_varchar && val_is_varchar) {
			// Fast path: UnifiedVectorFormat avoids per-cell Value allocation
			UnifiedVectorFormat key_fmt, val_fmt;
			chunk.data[key_col_idx].ToUnifiedFormat(chunk.size(), key_fmt);
			chunk.data[0].ToUnifiedFormat(chunk.size(), val_fmt);
			auto keys = UnifiedVectorFormat::GetData<string_t>(key_fmt);
			auto vals = UnifiedVectorFormat::GetData<string_t>(val_fmt);
			for (idx_t row = 0; row < chunk.size(); row++) {
				auto kidx = key_fmt.sel->get_index(row);
				auto vidx = val_fmt.sel->get_index(row);
				if (!key_fmt.validity.RowIsValid(kidx)) {
					continue;
				}
				std::string_view key(keys[kidx].GetData(), keys[kidx].GetSize());
				if (!val_fmt.validity.RowIsValid(vidx)) {
					batch.put(key, "");
				} else if (val_is_json) {
					Value v(std::string(vals[vidx].GetData(), vals[vidx].GetSize()));
					batch.put(key, TypedValueToJsonString(v, val_col_type));
				} else {
					batch.put(key, std::string_view(vals[vidx].GetData(), vals[vidx].GetSize()));
				}
				ctx.txn.CheckKeyAgainstTables(key, ctx.schema);
			}
		} else {
			// Slow path: non-VARCHAR columns go through Value::GetValue
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
		}
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
