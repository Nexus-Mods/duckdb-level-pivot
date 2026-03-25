#include "level_pivot_scan.hpp"
#include "level_pivot_table_entry.hpp" // includes LEVEL_PIVOT_VIRTUAL_COL_BASE
#include "level_pivot_utils.hpp"
#include "key_parser.hpp"
#include "level_pivot_storage.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include <algorithm>

namespace duckdb {

LevelPivotScanGlobalState::LevelPivotScanGlobalState() : done(false) {
}

// Mapping from attr name to output column index (sorted by name to match LevelDB order)
struct AttrMapping {
	std::string_view name;
	idx_t output_col;
	LogicalType type;
	bool is_json;
};

// Mapping from capture index to output column index
struct IdentityMapping {
	idx_t capture_index;
	idx_t output_col;
	LogicalType type;
};

struct LevelPivotScanLocalState : public LocalTableFunctionState {
	std::unique_ptr<level_pivot::LevelDBIterator> iterator;
	std::string prefix;
	bool initialized = false;

	// Zero-alloc parse buffers (reused every key)
	std::string_view captures_buf[level_pivot::MAX_KEY_CAPTURES];
	std::string_view attr_sv;

	// Reusable identity buffer (assign() reuses string capacity after first row)
	std::vector<std::string> current_identity;
	bool has_identity = false;
	size_t num_captures = 0;

	// Column lookup tables (built once at init)
	std::vector<AttrMapping> attr_mappings; // sorted by name to match LevelDB order
	std::vector<IdentityMapping> identity_mappings;

	// Per-row NULL tracking (one flag per attr column)
	std::vector<bool> attr_written;
};

static unique_ptr<FunctionData> LevelPivotBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	// This is called from GetScanFunction, bind_data is already set
	throw InternalException("LevelPivot scan should not be bound directly");
}

// Called during optimization to extract equality filters on identity columns.
// We inspect the expressions and store a narrowed prefix in bind_data for the scan to use.
// We leave all filters in place so DuckDB still applies them as a post-filter.
static void LevelPivotPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
                                            vector<unique_ptr<Expression>> &filters) {
	if (!bind_data) {
		return;
	}
	auto &scan_data = bind_data->Cast<LevelPivotScanData>();
	// Always reset prefix - bind_data may be reused across queries via Copy()
	scan_data.filter_prefix.clear();
	auto *table_entry = scan_data.table_entry;
	if (!table_entry || table_entry->GetTableMode() != LevelPivotTableMode::PIVOT) {
		return;
	}

	auto &parser = table_entry->GetKeyParser();
	auto &pattern = parser.pattern();
	auto &capture_names = pattern.capture_names();

	// Build a map: column_name -> equality_value from the filter expressions
	std::unordered_map<std::string, std::string> eq_values;
	for (idx_t i = 0; i < filters.size(); i++) {
		auto &filter = filters[i];
		if (filter->expression_class != ExpressionClass::BOUND_COMPARISON) {
			continue;
		}
		auto &comp = filter->Cast<BoundComparisonExpression>();
		if (comp.type != ExpressionType::COMPARE_EQUAL) {
			continue;
		}

		BoundColumnRefExpression *col_ref = nullptr;
		BoundConstantExpression *const_ref = nullptr;

		if (comp.left->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
		    comp.right->expression_class == ExpressionClass::BOUND_CONSTANT) {
			col_ref = &comp.left->Cast<BoundColumnRefExpression>();
			const_ref = &comp.right->Cast<BoundConstantExpression>();
		} else if (comp.right->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
		           comp.left->expression_class == ExpressionClass::BOUND_CONSTANT) {
			col_ref = &comp.right->Cast<BoundColumnRefExpression>();
			const_ref = &comp.left->Cast<BoundConstantExpression>();
		}

		if (!col_ref || !const_ref) {
			continue;
		}
		if (col_ref->binding.table_index != get.table_index) {
			continue;
		}
		if (const_ref->value.IsNull()) {
			continue;
		}

		// Map from output position through column_ids to actual table column index
		auto output_idx = col_ref->binding.column_index;
		auto &col_ids = get.GetColumnIds();
		if (output_idx >= col_ids.size()) {
			continue;
		}
		auto table_col_idx = col_ids[output_idx].GetPrimaryIndex();
		if (table_col_idx < get.names.size()) {
			eq_values[get.names[table_col_idx]] = const_ref->value.ToString();
		}
	}

	// Build prefix from consecutive identity column equality matches
	std::vector<std::string> capture_values;
	for (auto &cap_name : capture_names) {
		auto it = eq_values.find(cap_name);
		if (it == eq_values.end()) {
			break;
		}
		capture_values.push_back(it->second);
	}

	if (!capture_values.empty()) {
		scan_data.filter_prefix = parser.build_prefix(capture_values);
	}
}

static unique_ptr<GlobalTableFunctionState> LevelPivotInitGlobal(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto result = make_uniq<LevelPivotScanGlobalState>();
	result->column_ids = input.column_ids;

	// Copy filter prefix from bind_data (set by pushdown_complex_filter during optimization)
	if (input.bind_data) {
		auto &bind_data = input.bind_data->Cast<LevelPivotScanData>();
		result->filter_prefix = bind_data.filter_prefix;
	}

	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> LevelPivotInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	return make_uniq<LevelPivotScanLocalState>();
}

// Write a string_view directly into a DuckDB output vector (bypasses Value allocation for VARCHAR)
static inline void WriteStringDirect(Vector &vec, idx_t row, std::string_view sv) {
	FlatVector::GetData<string_t>(vec)[row] = StringVector::AddString(vec, sv.data(), sv.size());
}

static inline void WriteValueDirect(Vector &vec, idx_t row, std::string_view sv, const LogicalType &type,
                                    bool is_json = false) {
	if (is_json) {
		auto val = JsonStringToTypedValue(sv, type);
		if (val.IsNull()) {
			FlatVector::SetNull(vec, row, true);
		} else if (type.id() == LogicalTypeId::VARCHAR) {
			auto str = val.ToString();
			WriteStringDirect(vec, row, str);
		} else {
			vec.SetValue(row, val);
		}
		return;
	}
	if (type.id() == LogicalTypeId::VARCHAR) {
		WriteStringDirect(vec, row, sv);
	} else {
		vec.SetValue(row, StringToTypedValue(sv, type));
	}
}

// Update identity from captures, reusing string buffer capacity
static inline void UpdateIdentity(std::vector<std::string> &identity, const std::string_view *captures, size_t count) {
	identity.resize(count);
	for (size_t i = 0; i < count; ++i) {
		identity[i].assign(captures[i].data(), captures[i].size());
	}
}

static void PivotScan(LevelPivotTableEntry &table_entry, LevelPivotScanLocalState &lstate,
                      LevelPivotScanGlobalState &gstate, DataChunk &output, const vector<column_t> &column_ids) {
	auto &parser = table_entry.GetKeyParser();
	auto &connection = *table_entry.GetConnection();
	auto &columns = table_entry.GetColumns();

	if (!lstate.initialized) {
		// Use filter-narrowed prefix if available, otherwise use the full table prefix
		lstate.prefix = gstate.filter_prefix.empty() ? parser.build_prefix() : gstate.filter_prefix;
		lstate.iterator = std::make_unique<level_pivot::LevelDBIterator>(connection.iterator());
		if (lstate.prefix.empty()) {
			lstate.iterator->seek_to_first();
		} else {
			lstate.iterator->seek(lstate.prefix);
		}

		lstate.num_captures = parser.pattern().capture_count();

		// Build projection-aware column mappings
		auto &identity_cols = table_entry.GetIdentityColumns();
		auto &attr_cols = table_entry.GetAttrColumns();

		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto col_idx = column_ids[i];
			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				continue;
			}

			// Resolve virtual column IDs to real column indices
			if (col_idx >= LEVEL_PIVOT_VIRTUAL_COL_BASE) {
				auto identity_idx = col_idx - LEVEL_PIVOT_VIRTUAL_COL_BASE;
				if (identity_idx < identity_cols.size()) {
					auto &id_col_name = identity_cols[identity_idx];
					auto real_idx = table_entry.GetColumnIndex(id_col_name);
					auto &col = columns.GetColumn(LogicalIndex(real_idx));
					auto capture_idx = parser.pattern().capture_index(id_col_name);
					IdentityMapping im;
					im.capture_index = capture_idx >= 0 ? static_cast<idx_t>(capture_idx) : 0;
					im.output_col = i;
					im.type = col.Type();
					lstate.identity_mappings.push_back(std::move(im));
				}
				continue;
			}

			auto &col = columns.GetColumn(LogicalIndex(col_idx));
			auto &col_name = col.Name();

			if (std::find(identity_cols.begin(), identity_cols.end(), col_name) != identity_cols.end()) {
				auto capture_idx = parser.pattern().capture_index(col_name);
				IdentityMapping im;
				im.capture_index = capture_idx >= 0 ? static_cast<idx_t>(capture_idx) : 0;
				im.output_col = i;
				im.type = col.Type();
				lstate.identity_mappings.push_back(std::move(im));
			} else if (std::find(attr_cols.begin(), attr_cols.end(), col_name) != attr_cols.end()) {
				AttrMapping am;
				am.name = col_name;
				am.output_col = i;
				am.type = col.Type();
				am.is_json = table_entry.IsJsonColumn(col_idx);
				lstate.attr_mappings.push_back(std::move(am));
			}
		}

		// Sort attr_mappings by name to match LevelDB's sorted key order
		std::sort(lstate.attr_mappings.begin(), lstate.attr_mappings.end(),
		          [](const AttrMapping &a, const AttrMapping &b) { return a.name < b.name; });

		lstate.attr_written.resize(lstate.attr_mappings.size(), false);
		lstate.initialized = true;
	}

	auto num_captures = lstate.num_captures;
	auto &attr_mappings = lstate.attr_mappings;
	auto num_attrs = attr_mappings.size();

	idx_t count = 0;
	while (lstate.iterator && lstate.iterator->valid()) {
		std::string_view key_sv = lstate.iterator->key_view();

		if (!IsWithinPrefix(key_sv, lstate.prefix)) {
			if (!lstate.has_identity) {
				gstate.done = true;
				break;
			}
			// Finalize last row: set NULLs for unwritten attrs
			for (size_t a = 0; a < num_attrs; ++a) {
				if (!lstate.attr_written[a]) {
					FlatVector::SetNull(output.data[attr_mappings[a].output_col], count, true);
				}
			}
			count++;
			lstate.has_identity = false;
			gstate.done = true;
			break;
		}

		// Parse key with zero-alloc fast path
		if (!parser.parse_fast(key_sv, lstate.captures_buf, lstate.attr_sv)) {
			lstate.iterator->next();
			continue;
		}

		if (!lstate.has_identity) {
			// First key - start new row
			UpdateIdentity(lstate.current_identity, lstate.captures_buf, num_captures);
			lstate.has_identity = true;
			std::fill(lstate.attr_written.begin(), lstate.attr_written.end(), false);

			// Write identity columns directly
			for (auto &im : lstate.identity_mappings) {
				WriteValueDirect(output.data[im.output_col], count, lstate.captures_buf[im.capture_index], im.type);
			}
		} else if (!IdentityMatches(lstate.current_identity, lstate.captures_buf, num_captures)) {
			// Identity changed - finalize previous row
			for (size_t a = 0; a < num_attrs; ++a) {
				if (!lstate.attr_written[a]) {
					FlatVector::SetNull(output.data[attr_mappings[a].output_col], count, true);
				}
			}
			count++;

			if (count >= STANDARD_VECTOR_SIZE) {
				// Chunk full - save new identity for next chunk
				UpdateIdentity(lstate.current_identity, lstate.captures_buf, num_captures);
				std::fill(lstate.attr_written.begin(), lstate.attr_written.end(), false);

				// Write identity columns for next row (will be row 0 of next chunk)
				// Actually, we need to NOT advance the iterator, so the next call picks up here.
				// But we already parsed this key. We need to write this key's data into the next chunk.
				// Solution: don't advance iterator, set identity, and return.
				// The next call to PivotScan will re-parse this key and handle it.
				lstate.has_identity = false;
				output.SetCardinality(count);
				return;
			}

			// Start new row
			UpdateIdentity(lstate.current_identity, lstate.captures_buf, num_captures);
			std::fill(lstate.attr_written.begin(), lstate.attr_written.end(), false);

			// Write identity columns directly
			for (auto &im : lstate.identity_mappings) {
				WriteValueDirect(output.data[im.output_col], count, lstate.captures_buf[im.capture_index], im.type);
			}
		}

		// Find attr in sorted attr_mappings (linear scan, typically 2-5 entries)
		for (size_t a = 0; a < num_attrs; ++a) {
			if (attr_mappings[a].name == lstate.attr_sv) {
				std::string_view val_sv = lstate.iterator->value_view();
				WriteValueDirect(output.data[attr_mappings[a].output_col], count, val_sv, attr_mappings[a].type,
				                 attr_mappings[a].is_json);
				lstate.attr_written[a] = true;
				break;
			}
		}

		lstate.iterator->next();
	}

	// Iterator exhausted - finalize last row if any
	if (lstate.has_identity) {
		for (size_t a = 0; a < num_attrs; ++a) {
			if (!lstate.attr_written[a]) {
				FlatVector::SetNull(output.data[attr_mappings[a].output_col], count, true);
			}
		}
		count++;
		lstate.has_identity = false;
		gstate.done = true;
	}

	if (!lstate.iterator || !lstate.iterator->valid()) {
		if (lstate.iterator) {
			auto err = lstate.iterator->status();
			if (!err.empty()) {
				throw IOException("LevelDB iterator error during pivot scan (returned %llu rows before failing): %s",
				                  (unsigned long long)count, err);
			}
		}
		gstate.done = true;
	}

	output.SetCardinality(count);
}

static void RawScan(LevelPivotTableEntry &table_entry, LevelPivotScanLocalState &lstate,
                    LevelPivotScanGlobalState &gstate, DataChunk &output, const vector<column_t> &column_ids) {
	auto &connection = *table_entry.GetConnection();
	auto &columns = table_entry.GetColumns();

	if (!lstate.initialized) {
		lstate.iterator = std::make_unique<level_pivot::LevelDBIterator>(connection.iterator());
		lstate.iterator->seek_to_first();
		lstate.initialized = true;
	}

	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE && lstate.iterator && lstate.iterator->valid()) {
		std::string_view key_sv = lstate.iterator->key_view();
		std::string_view val_sv = lstate.iterator->value_view();

		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto col_idx = column_ids[i];
			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				continue;
			}
			// Resolve virtual column IDs: LEVEL_PIVOT_VIRTUAL_COL_BASE + 0 = key column
			if (col_idx >= LEVEL_PIVOT_VIRTUAL_COL_BASE) {
				col_idx = col_idx - LEVEL_PIVOT_VIRTUAL_COL_BASE;
			}
			auto &col_type = columns.GetColumn(LogicalIndex(col_idx)).Type();
			bool is_json = table_entry.IsJsonColumn(col_idx);
			if (col_idx == 0) {
				WriteValueDirect(output.data[i], count, key_sv, col_type);
			} else if (col_idx == 1) {
				WriteValueDirect(output.data[i], count, val_sv, col_type, is_json);
			}
		}
		count++;
		lstate.iterator->next();
	}

	if (!lstate.iterator || !lstate.iterator->valid()) {
		if (lstate.iterator) {
			auto err = lstate.iterator->status();
			if (!err.empty()) {
				throw IOException("LevelDB iterator error during raw scan (returned %llu rows before failing): %s",
				                  (unsigned long long)count, err);
			}
		}
		gstate.done = true;
	}

	output.SetCardinality(count);
}

static void LevelPivotScanFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<LevelPivotScanData>();
	auto &gstate = data.global_state->Cast<LevelPivotScanGlobalState>();
	auto &lstate = data.local_state->Cast<LevelPivotScanLocalState>();
	auto &table_entry = *bind_data.table_entry;

	if (gstate.done) {
		output.SetCardinality(0);
		return;
	}

	auto &column_ids = gstate.column_ids;

	if (table_entry.GetTableMode() == LevelPivotTableMode::PIVOT) {
		PivotScan(table_entry, lstate, gstate, output, column_ids);
	} else {
		RawScan(table_entry, lstate, gstate, output, column_ids);
	}
}

static BindInfo LevelPivotGetBindInfo(const optional_ptr<FunctionData> bind_data) {
	auto &scan_data = bind_data->Cast<LevelPivotScanData>();
	return BindInfo(*scan_data.table_entry);
}

TableFunction LevelPivotScanFunction() {
	TableFunction func("level_pivot_scan", {}, LevelPivotScanFunc);
	func.init_global = LevelPivotInitGlobal;
	func.init_local = LevelPivotInitLocal;
	func.projection_pushdown = true;
	func.filter_pushdown = false;
	func.pushdown_complex_filter = LevelPivotPushdownComplexFilter;
	func.get_bind_info = LevelPivotGetBindInfo;
	return func;
}

} // namespace duckdb
