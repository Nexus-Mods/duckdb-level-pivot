#include "level_pivot_insert.hpp"
#include "key_parser.hpp"
#include "level_pivot_sink_helpers.hpp"
#include "level_pivot_utils.hpp"

namespace duckdb {

LevelPivotInsert::LevelPivotInsert(PhysicalPlan &plan, vector<LogicalType> types, TableCatalogEntry &table,
                                   idx_t estimated_cardinality)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality), table(table) {
}

unique_ptr<GlobalSinkState> LevelPivotInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<LevelPivotSinkGlobalState>();
}

SinkResultType LevelPivotInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<LevelPivotSinkGlobalState>();
	auto ctx = GetSinkContext(context, table);

	if (ctx.table.GetTableMode() == LevelPivotTableMode::PIVOT) {
		auto &parser = ctx.table.GetKeyParser();
		auto &attr_cols = ctx.table.GetAttrColumns();

		auto batch = ctx.connection.create_batch();
		auto &capture_names = parser.pattern().capture_names();

		std::vector<std::string> identity_values;
		identity_values.reserve(capture_names.size());

		for (idx_t row = 0; row < chunk.size(); row++) {
			// Extract identity values in capture order
			identity_values.clear();
			for (auto &cap_name : capture_names) {
				auto col_idx = ctx.table.GetColumnIndex(cap_name);
				auto val = chunk.data[col_idx].GetValue(row);
				if (val.IsNull()) {
					throw InvalidInputException("Cannot insert NULL into identity column '%s'", cap_name);
				}
				identity_values.push_back(val.ToString());
			}

			// Write a key for each non-null attr column
			for (auto &attr_name : attr_cols) {
				auto col_idx = ctx.table.GetColumnIndex(attr_name);
				auto val = chunk.data[col_idx].GetValue(row);
				if (!val.IsNull()) {
					std::string key = parser.build(identity_values, attr_name);
					if (ctx.table.IsJsonColumn(col_idx)) {
						auto &col_type = ctx.table.GetColumns().GetColumn(LogicalIndex(col_idx)).Type();
						batch.put(key, TypedValueToJsonString(val, col_type));
					} else {
						batch.put(key, val.ToString());
					}
					ctx.txn.CheckKeyAgainstTables(key, ctx.schema);
				}
			}
		}

		batch.commit();
		gstate.row_count += chunk.size();
	} else {
		// Raw mode: column 0 = key, column 1 = value
		bool val_is_json = ctx.table.IsJsonColumn(1);
		auto &val_col_type = ctx.table.GetColumns().GetColumn(LogicalIndex(1)).Type();
		auto batch = ctx.connection.create_batch();
		for (idx_t row = 0; row < chunk.size(); row++) {
			auto key_val = chunk.data[0].GetValue(row);
			auto val_val = chunk.data[1].GetValue(row);
			if (key_val.IsNull()) {
				throw InvalidInputException("Cannot insert NULL key in raw mode");
			}
			std::string key = key_val.ToString();
			if (val_val.IsNull()) {
				batch.put(key, "");
			} else if (val_is_json) {
				batch.put(key, TypedValueToJsonString(val_val, val_col_type));
			} else {
				batch.put(key, val_val.ToString());
			}
			ctx.txn.CheckKeyAgainstTables(key, ctx.schema);
		}
		batch.commit();
		gstate.row_count += chunk.size();
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType LevelPivotInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	return SinkFinalizeType::READY;
}

SourceResultType LevelPivotInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	return EmitRowCount(*sink_state, chunk);
}

} // namespace duckdb
