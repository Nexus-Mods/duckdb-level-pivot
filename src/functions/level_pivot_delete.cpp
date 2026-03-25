#include "level_pivot_delete.hpp"
#include "key_parser.hpp"
#include "level_pivot_sink_helpers.hpp"
#include "level_pivot_utils.hpp"

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

	if (ctx.table.GetTableMode() == LevelPivotTableMode::PIVOT) {
		auto &parser = ctx.table.GetKeyParser();
		auto batch = ctx.connection.create_batch();
		auto iter = ctx.connection.iterator();

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

		batch.commit();
		gstate.row_count += chunk.size();
	} else {
		auto batch = ctx.connection.create_batch();
		for (idx_t row = 0; row < chunk.size(); row++) {
			auto key_val = chunk.data[0].GetValue(row);
			if (!key_val.IsNull()) {
				auto key = key_val.ToString();
				batch.del(key);
				ctx.txn.CheckKeyAgainstTables(key, ctx.schema);
			}
		}
		batch.commit();
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
