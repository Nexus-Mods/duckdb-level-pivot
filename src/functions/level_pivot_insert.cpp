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
	ctx.txn.MarkTableDirty(ctx.table.name);

	if (ctx.table.GetTableMode() == LevelPivotTableMode::PIVOT) {
		auto &parser = ctx.table.GetKeyParser();
		auto &attr_cols = ctx.table.GetAttrColumns();

		auto batch = GetSinkBatch(ctx);
		auto &capture_names = parser.pattern().capture_names();
		auto &columns_view = ctx.table.GetColumns();

		// Pre-resolve capture column indices (in capture order)
		std::vector<idx_t> capture_col_indices;
		capture_col_indices.reserve(capture_names.size());
		for (auto &n : capture_names) {
			capture_col_indices.push_back(ctx.table.GetColumnIndex(n));
		}

		// Pre-resolve attr column indices, JSON flags, types (in attr_cols order)
		std::vector<idx_t> attr_col_indices;
		std::vector<bool> attr_is_json;
		std::vector<LogicalType> attr_types;
		attr_col_indices.reserve(attr_cols.size());
		attr_is_json.reserve(attr_cols.size());
		attr_types.reserve(attr_cols.size());
		for (auto &n : attr_cols) {
			auto idx = ctx.table.GetColumnIndex(n);
			attr_col_indices.push_back(idx);
			attr_is_json.push_back(ctx.table.IsJsonColumn(idx));
			attr_types.push_back(columns_view.GetColumn(LogicalIndex(idx)).Type());
		}

		// For each capture column: pre-build UVF if it's VARCHAR, else use slow path
		struct CaptureFmt {
			bool fast_path = false; // true → plain VARCHAR, use UVF
			UnifiedVectorFormat fmt;
			const string_t *data = nullptr;
		};
		std::vector<CaptureFmt> capture_fmts(capture_names.size());
		for (size_t c = 0; c < capture_names.size(); c++) {
			auto cap_idx = capture_col_indices[c];
			bool is_varchar = columns_view.GetColumn(LogicalIndex(cap_idx)).Type().id() == LogicalTypeId::VARCHAR;
			if (is_varchar) {
				capture_fmts[c].fast_path = true;
				chunk.data[cap_idx].ToUnifiedFormat(chunk.size(), capture_fmts[c].fmt);
				capture_fmts[c].data = UnifiedVectorFormat::GetData<string_t>(capture_fmts[c].fmt);
			}
		}

		// For each attr: pre-build UnifiedVectorFormat if it's plain VARCHAR (not JSON)
		struct AttrFmt {
			bool fast_path = false;
			UnifiedVectorFormat fmt;
			const string_t *data = nullptr;
		};
		std::vector<AttrFmt> attr_fmts(attr_cols.size());
		for (size_t a = 0; a < attr_cols.size(); a++) {
			bool is_varchar = attr_types[a].id() == LogicalTypeId::VARCHAR;
			if (is_varchar && !attr_is_json[a]) {
				attr_fmts[a].fast_path = true;
				chunk.data[attr_col_indices[a]].ToUnifiedFormat(chunk.size(), attr_fmts[a].fmt);
				attr_fmts[a].data = UnifiedVectorFormat::GetData<string_t>(attr_fmts[a].fmt);
			}
		}

		std::vector<std::string> identity_values;
		identity_values.reserve(capture_names.size());
		std::string key_buf;

		for (idx_t row = 0; row < chunk.size(); row++) {
			// Extract identity values in capture order
			identity_values.clear();
			for (size_t c = 0; c < capture_names.size(); c++) {
				if (capture_fmts[c].fast_path) {
					auto cidx = capture_fmts[c].fmt.sel->get_index(row);
					if (!capture_fmts[c].fmt.validity.RowIsValid(cidx)) {
						throw InvalidInputException("Cannot insert NULL into identity column '%s'", capture_names[c]);
					}
					identity_values.emplace_back(capture_fmts[c].data[cidx].GetData(),
					                             capture_fmts[c].data[cidx].GetSize());
				} else {
					// Slow path: non-VARCHAR identity column
					auto val = chunk.data[capture_col_indices[c]].GetValue(row);
					if (val.IsNull()) {
						throw InvalidInputException("Cannot insert NULL into identity column '%s'", capture_names[c]);
					}
					identity_values.push_back(val.ToString());
				}
			}

			// Build the identity prefix once per row; reuse for every attr
			key_buf.clear();
			parser.build_identity_prefix_with_delim_into(key_buf, identity_values);
			size_t prefix_len = key_buf.size();
			const auto &tail = parser.attr_tail();

			// Write a key for each non-null attr column
			for (size_t a = 0; a < attr_cols.size(); a++) {
				if (attr_fmts[a].fast_path) {
					auto &fmt = attr_fmts[a].fmt;
					auto vidx = fmt.sel->get_index(row);
					if (!fmt.validity.RowIsValid(vidx)) {
						continue;
					}
					key_buf.resize(prefix_len);
					key_buf += attr_cols[a];
					key_buf += tail;
					std::string_view value(attr_fmts[a].data[vidx].GetData(), attr_fmts[a].data[vidx].GetSize());
					batch.put(key_buf, value);
					ctx.txn.CheckKeyAgainstTables(key_buf, ctx.schema);
				} else {
					// Slow path: typed or JSON columns still go through Value
					auto val = chunk.data[attr_col_indices[a]].GetValue(row);
					if (val.IsNull()) {
						continue;
					}
					key_buf.resize(prefix_len);
					key_buf += attr_cols[a];
					key_buf += tail;
					if (attr_is_json[a]) {
						batch.put(key_buf, TypedValueToJsonString(val, attr_types[a]));
					} else {
						batch.put(key_buf, val.ToString());
					}
					ctx.txn.CheckKeyAgainstTables(key_buf, ctx.schema);
				}
			}
		}

		gstate.row_count += chunk.size();
	} else {
		// Raw mode: column 0 = key, column 1 = value
		auto &key_col_type = ctx.table.GetColumns().GetColumn(LogicalIndex(0)).Type();
		auto &val_col_type = ctx.table.GetColumns().GetColumn(LogicalIndex(1)).Type();
		bool key_is_varchar = key_col_type.id() == LogicalTypeId::VARCHAR;
		bool val_is_varchar = val_col_type.id() == LogicalTypeId::VARCHAR;
		bool val_is_json = ctx.table.IsJsonColumn(1);
		auto batch = GetSinkBatch(ctx);

		if (key_is_varchar && val_is_varchar) {
			// Fast path: UnifiedVectorFormat avoids per-cell Value allocation
			UnifiedVectorFormat key_fmt, val_fmt;
			chunk.data[0].ToUnifiedFormat(chunk.size(), key_fmt);
			chunk.data[1].ToUnifiedFormat(chunk.size(), val_fmt);
			auto keys = UnifiedVectorFormat::GetData<string_t>(key_fmt);
			auto vals = UnifiedVectorFormat::GetData<string_t>(val_fmt);
			for (idx_t row = 0; row < chunk.size(); row++) {
				auto kidx = key_fmt.sel->get_index(row);
				auto vidx = val_fmt.sel->get_index(row);
				if (!key_fmt.validity.RowIsValid(kidx)) {
					throw InvalidInputException("Cannot insert NULL key in raw mode");
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
		}
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
