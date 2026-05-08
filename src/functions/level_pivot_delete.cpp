#include "level_pivot_delete.hpp"
#include "key_parser.hpp"
#include "level_pivot_sink_helpers.hpp"
#include "level_pivot_utils.hpp"
#include "level_pivot_overlay.hpp"

namespace duckdb {

LevelPivotDelete::LevelPivotDelete(PhysicalPlan &plan, vector<LogicalType> types, TableCatalogEntry &table,
                                   idx_t estimated_cardinality)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality), table(table) {
}

unique_ptr<GlobalSinkState> LevelPivotDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<LevelPivotSinkGlobalState>();
}

SinkResultType LevelPivotDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<LevelPivotSinkGlobalState>();
	auto ctx = GetSinkContext(context, table);
	ctx.txn.MarkTableDirty(ctx.table.name);

	if (ctx.table.GetTableMode() == LevelPivotTableMode::PIVOT) {
		auto &parser = ctx.table.GetKeyParser();
		auto batch = GetSinkBatch(ctx);
		auto base_iter = ctx.connection.iterator();
		level_pivot::MergedIterator iter(std::move(base_iter), &ctx.txn.Overlay());

		std::vector<std::string> identity_values;
		identity_values.reserve(chunk.ColumnCount());

		auto row_id_cols = ctx.table.GetRowIdColumns();
		idx_t num_row_id_cols = row_id_cols.size();
		idx_t row_id_offset = chunk.ColumnCount() - num_row_id_cols;

		for (idx_t row = 0; row < chunk.size(); row++) {
			// The row ID columns are at the END of the chunk (added by BindRowIdColumns)
			ExtractIdentityValues(identity_values, chunk, row, row_id_offset, num_row_id_cols);

			// Find all keys matching this identity and delete them
			std::string prefix = parser.build_prefix(identity_values);
			if (prefix.empty()) {
				iter.seek_to_first();
			} else {
				iter.seek(prefix);
			}

			while (iter.valid()) {
				std::string_view key_sv = iter.key_view();
				if (!IsWithinPrefix(key_sv, prefix)) {
					break;
				}

				auto parsed = parser.parse_view(key_sv);
				if (parsed &&
				    IdentityMatches(identity_values, parsed->capture_values.data(), parsed->capture_values.size())) {
					batch.del(key_sv);
					ctx.txn.CheckKeyAgainstTables(key_sv, ctx.schema);
				}
				iter.next();
			}
		}

		gstate.row_count += chunk.size();
	} else {
		auto &key_col_type = ctx.table.GetColumns().GetColumn(LogicalIndex(0)).Type();
		bool key_is_varchar = key_col_type.id() == LogicalTypeId::VARCHAR;
		auto batch = GetSinkBatch(ctx);

		if (key_is_varchar) {
			// Fast path: UnifiedVectorFormat avoids per-cell Value allocation
			UnifiedVectorFormat key_fmt;
			chunk.data[0].ToUnifiedFormat(chunk.size(), key_fmt);
			auto keys = UnifiedVectorFormat::GetData<string_t>(key_fmt);
			for (idx_t row = 0; row < chunk.size(); row++) {
				auto kidx = key_fmt.sel->get_index(row);
				if (!key_fmt.validity.RowIsValid(kidx)) {
					continue;
				}
				std::string_view key(keys[kidx].GetData(), keys[kidx].GetSize());
				batch.del(key);
				ctx.txn.CheckKeyAgainstTables(key, ctx.schema);
			}
		} else {
			// Slow path: non-VARCHAR key column goes through Value::GetValue
			for (idx_t row = 0; row < chunk.size(); row++) {
				auto key_val = chunk.data[0].GetValue(row);
				if (!key_val.IsNull()) {
					auto key = key_val.ToString();
					batch.del(key);
					ctx.txn.CheckKeyAgainstTables(key, ctx.schema);
				}
			}
		}
		gstate.row_count += chunk.size();
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType LevelPivotDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	return SinkFinalizeType::READY;
}

SourceResultType LevelPivotDelete::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	return EmitRowCount(*sink_state, chunk);
}

} // namespace duckdb
