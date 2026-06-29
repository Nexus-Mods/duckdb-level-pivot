#include "level_pivot_scan.hpp"
#include "level_pivot_table_entry.hpp" // includes LEVEL_PIVOT_VIRTUAL_COL_BASE
#include "level_pivot_utils.hpp"
#include "key_parser.hpp"
#include "level_pivot_storage.hpp"
#include "level_pivot_overlay.hpp"
#include "level_pivot_catalog.hpp"
#include "level_pivot_transaction.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
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
	std::unique_ptr<level_pivot::MergedIterator> iterator;
	const level_pivot::TransactionOverlay *overlay = nullptr; // weak, lifetime = txn
	std::string prefix;
	bool initialized = false;
	idx_t point_key_index = 0;  // cursor for raw-mode point-lookup path
	idx_t prefix_index = 0;     // cursor for raw-mode prefix range-seek path
	bool prefix_seeked = false; // whether the iterator is seeked to prefixes[prefix_index]
	idx_t range_index = 0;      // cursor for raw-mode bounded range-seek path
	bool range_seeked = false;  // whether the iterator is seeked to ranges[range_index].lo

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

// True if `expr` is a column ref bound to this scan's key column (raw mode column 0).
static bool IsKeyColumnRef(Expression &expr, LogicalGet &get, const string &key_col_name) {
	if (expr.expression_class != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto &col_ref = expr.Cast<BoundColumnRefExpression>();
	if (col_ref.binding.table_index != get.table_index) {
		return false;
	}
	auto output_idx = col_ref.binding.column_index;
	auto &col_ids = get.GetColumnIds();
	if (output_idx >= col_ids.size()) {
		return false;
	}
	auto table_col_idx = col_ids[output_idx].GetPrimaryIndex();
	if (table_col_idx >= get.names.size()) {
		return false;
	}
	return get.names[table_col_idx] == key_col_name;
}

// Map a comparison to the equivalent one with the operands swapped (`'x' < key` => `key > 'x'`).
static ExpressionType FlipComparison(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_LESSTHAN:
		return ExpressionType::COMPARE_GREATERTHAN;
	case ExpressionType::COMPARE_GREATERTHAN:
		return ExpressionType::COMPARE_LESSTHAN;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return ExpressionType::COMPARE_GREATERTHANOREQUALTO;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return ExpressionType::COMPARE_LESSTHANOREQUALTO;
	default:
		return type;
	}
}

// If `comp` compares the key column against a non-null constant, return true and set `out_value` to
// the constant's string form and `out_type` to the comparison oriented as `key <op> const` (operands
// flipped when the constant is on the left). Shared by the equality and range pushdown paths so both
// agree on operand orientation, key-column matching, and NULL handling.
static bool MatchKeyConstComparison(BoundComparisonExpression &comp, LogicalGet &get, const string &key_col_name,
                                    string &out_value, ExpressionType &out_type) {
	Expression *col = comp.left.get();
	Expression *cst = comp.right.get();
	out_type = comp.type;
	if (cst->expression_class != ExpressionClass::BOUND_CONSTANT) {
		std::swap(col, cst);
		out_type = FlipComparison(out_type);
	}
	if (cst->expression_class != ExpressionClass::BOUND_CONSTANT || !IsKeyColumnRef(*col, get, key_col_name)) {
		return false;
	}
	auto &cv = cst->Cast<BoundConstantExpression>();
	if (cv.value.IsNull()) {
		return false;
	}
	out_value = cv.value.ToString();
	return true;
}

// Recursively extract a fully-recognised raw-mode key predicate into points/prefixes.
// Recognised forms: `key = const`, `key IN (const, ...)`, `starts_with(key, const)`, and an OR
// of any of these. Returns false the moment anything is unrecognised; the caller then discards
// its temp output, so the scan never narrows on an under-approximation (a missed row). Each
// recognised conjunct is a *necessary* condition, so its extracted set is a superset of the
// matching rows; DuckDB still applies the original filter as a post-filter for exactness.
static bool ExtractKeyPredicate(Expression &expr, LogicalGet &get, const string &key_col_name,
                                vector<string> &out_points, vector<string> &out_prefixes) {
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_COMPARISON: {
		string value;
		ExpressionType type;
		if (!MatchKeyConstComparison(expr.Cast<BoundComparisonExpression>(), get, key_col_name, value, type) ||
		    type != ExpressionType::COMPARE_EQUAL) {
			return false;
		}
		out_points.push_back(std::move(value));
		return true;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expr.Cast<BoundOperatorExpression>();
		if (op.type != ExpressionType::COMPARE_IN || op.children.size() < 2) {
			return false;
		}
		if (!IsKeyColumnRef(*op.children[0], get, key_col_name)) {
			return false;
		}
		for (size_t k = 1; k < op.children.size(); k++) {
			if (op.children[k]->expression_class != ExpressionClass::BOUND_CONSTANT) {
				return false;
			}
		}
		for (size_t k = 1; k < op.children.size(); k++) {
			auto &cv = op.children[k]->Cast<BoundConstantExpression>();
			if (!cv.value.IsNull()) {
				out_points.push_back(cv.value.ToString());
			}
		}
		return true;
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &fn = expr.Cast<BoundFunctionExpression>();
		if ((fn.function.name != "starts_with" && fn.function.name != "prefix") || fn.children.size() != 2) {
			return false;
		}
		if (!IsKeyColumnRef(*fn.children[0], get, key_col_name) ||
		    fn.children[1]->expression_class != ExpressionClass::BOUND_CONSTANT) {
			return false;
		}
		auto &cv = fn.children[1]->Cast<BoundConstantExpression>();
		if (cv.value.IsNull()) {
			return false;
		}
		auto prefix = cv.value.ToString();
		if (prefix.empty()) {
			return false; // empty prefix matches the whole table; no point narrowing
		}
		out_prefixes.push_back(std::move(prefix));
		return true;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		if (conj.type != ExpressionType::CONJUNCTION_OR) {
			return false; // only OR is a safe union; AND would need intersection logic
		}
		for (auto &child : conj.children) {
			if (!ExtractKeyPredicate(*child, get, key_col_name, out_points, out_prefixes)) {
				return false;
			}
		}
		return true;
	}
	default:
		return false;
	}
}

// Tighten a range's lower bound: keep the largest (most selective) lower bound; on a tie an
// exclusive bound (`>`) is tighter than an inclusive one (`>=`).
static void ApplyLower(RawKeyRange &r, string val, bool inclusive) {
	if (!r.has_lo || val > r.lo || (val == r.lo && r.lo_inclusive && !inclusive)) {
		r.lo = std::move(val);
		r.lo_inclusive = inclusive;
		r.has_lo = true;
	}
}

// Tighten a range's upper bound: keep the smallest upper bound; on a tie an exclusive bound (`<`)
// is tighter than an inclusive one (`<=`).
static void ApplyUpper(RawKeyRange &r, string val, bool inclusive) {
	if (!r.has_hi || val < r.hi || (val == r.hi && r.hi_inclusive && !inclusive)) {
		r.hi = std::move(val);
		r.hi_inclusive = inclusive;
		r.has_hi = true;
	}
}

// Fold a constant bound on the key column into `range`. Handles `key </<=/>/>= const` comparisons
// and `key BETWEEN lo AND hi` (which DuckDB also produces by folding `key >= lo AND key <= hi`).
// A no-op for anything else, so the caller can pass every top-level filter.
static void ApplyKeyRangeBound(Expression &expr, LogicalGet &get, const string &key_col_name, RawKeyRange &range) {
	if (expr.expression_class == ExpressionClass::BOUND_BETWEEN) {
		auto &btw = expr.Cast<BoundBetweenExpression>();
		if (!IsKeyColumnRef(*btw.input, get, key_col_name)) {
			return;
		}
		if (btw.lower && btw.lower->expression_class == ExpressionClass::BOUND_CONSTANT) {
			auto &lv = btw.lower->Cast<BoundConstantExpression>();
			if (!lv.value.IsNull()) {
				ApplyLower(range, lv.value.ToString(), btw.lower_inclusive);
			}
		}
		if (btw.upper && btw.upper->expression_class == ExpressionClass::BOUND_CONSTANT) {
			auto &uv = btw.upper->Cast<BoundConstantExpression>();
			if (!uv.value.IsNull()) {
				ApplyUpper(range, uv.value.ToString(), btw.upper_inclusive);
			}
		}
		return;
	}
	if (expr.expression_class != ExpressionClass::BOUND_COMPARISON) {
		return;
	}
	string val;
	ExpressionType type;
	if (!MatchKeyConstComparison(expr.Cast<BoundComparisonExpression>(), get, key_col_name, val, type)) {
		return;
	}
	switch (type) {
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		ApplyLower(range, std::move(val), true);
		return;
	case ExpressionType::COMPARE_GREATERTHAN:
		ApplyLower(range, std::move(val), false);
		return;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		ApplyUpper(range, std::move(val), true);
		return;
	case ExpressionType::COMPARE_LESSTHAN:
		ApplyUpper(range, std::move(val), false);
		return;
	default:
		return;
	}
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
	// Always reset - bind_data may be reused across queries via Copy()
	scan_data.filter_prefix.clear();
	scan_data.point_keys.clear();
	scan_data.prefixes.clear();
	scan_data.ranges.clear();
	auto *table_entry = scan_data.table_entry;
	if (!table_entry) {
		return;
	}

	if (table_entry->GetTableMode() == LevelPivotTableMode::PIVOT) {
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
	} else {
		// RAW mode: column 0 is the key column. Push down `key = X`, `key IN (...)`,
		// `starts_with(key, P)`, and ORs of these (the subtree-delete shape) into point_keys and
		// prefixes so the scan seeks them instead of full-scanning. Filters are left in place, so
		// DuckDB still post-filters for exactness.
		auto &columns = table_entry->GetColumns();
		auto &key_col = columns.GetColumn(LogicalIndex(0));
		auto &key_col_name = key_col.Name();

		for (idx_t i = 0; i < filters.size(); i++) {
			// Extract into temporaries; adopt them only if the whole conjunct is recognised, so a
			// partially-understood OR never narrows the scan into missing a matching row.
			vector<string> tmp_points;
			vector<string> tmp_prefixes;
			if (ExtractKeyPredicate(*filters[i], get, key_col_name, tmp_points, tmp_prefixes)) {
				for (auto &p : tmp_points) {
					scan_data.point_keys.push_back(std::move(p));
				}
				for (auto &p : tmp_prefixes) {
					scan_data.prefixes.push_back(std::move(p));
				}
			}
		}

		// Range pass: the top-level filters are AND-ed, so fold every comparison / BETWEEN bound on
		// the key among them into a single bounded range by INTERSECTING bounds (tightest lo,
		// tightest hi). Unioning them (as ExtractKeyPredicate does for OR) would yield the whole
		// table for a two-sided range, so these bounds are handled only here, at AND scope.
		//
		// Only safe for VARCHAR keys: a range-seek walks LevelDB in lexicographic byte order, but
		// non-VARCHAR keys are stored as their decimal/text form, whose byte order differs from value
		// order (e.g. '10' < '2'). A lexicographic seek would skip rows the post-filter can't recover,
		// so for non-VARCHAR keys we leave the scan unfiltered and rely on the post-filter. (Point and
		// IN pushdown stay correct for any type — they match exact byte strings, not an order.)
		if (key_col.Type().id() == LogicalTypeId::VARCHAR) {
			RawKeyRange range;
			for (idx_t i = 0; i < filters.size(); i++) {
				ApplyKeyRangeBound(*filters[i], get, key_col_name, range);
			}
			if (range.has_lo || range.has_hi) {
				scan_data.ranges.push_back(std::move(range));
			}
		}
	}
}

static unique_ptr<GlobalTableFunctionState> LevelPivotInitGlobal(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto result = make_uniq<LevelPivotScanGlobalState>();
	result->column_ids = input.column_ids;

	// Copy filter prefix, point_keys and prefixes from bind_data (set by pushdown during optimization)
	if (input.bind_data) {
		auto &bind_data = input.bind_data->Cast<LevelPivotScanData>();
		result->filter_prefix = bind_data.filter_prefix;
		result->point_keys = bind_data.point_keys;
		result->prefixes = bind_data.prefixes;
		result->ranges = bind_data.ranges;

		// Normalise prefixes: sort + dedupe, then drop any prefix nested inside an earlier (shorter)
		// one. After this the prefix ranges are pairwise disjoint, so the prefix scan emits each row
		// once. Sorted order also lets the scan seek them in a single forward pass.
		std::sort(result->prefixes.begin(), result->prefixes.end());
		result->prefixes.erase(std::unique(result->prefixes.begin(), result->prefixes.end()), result->prefixes.end());
		vector<string> kept;
		for (auto &p : result->prefixes) {
			// In sorted order, a nested prefix is covered iff it starts with the last kept prefix.
			if (kept.empty() || !IsWithinPrefix(p, kept.back())) {
				kept.push_back(p);
			}
		}
		result->prefixes = std::move(kept);

		// Dedupe point_keys (e.g. WHERE k IN ('a','a')); drop any covered by a prefix range so the
		// point lookups and the prefix range-seek never emit the same row twice.
		std::sort(result->point_keys.begin(), result->point_keys.end());
		result->point_keys.erase(std::unique(result->point_keys.begin(), result->point_keys.end()),
		                         result->point_keys.end());
		if (!result->prefixes.empty()) {
			auto &prefixes = result->prefixes;
			result->point_keys.erase(std::remove_if(result->point_keys.begin(), result->point_keys.end(),
			                                        [&](const string &k) {
				                                        for (auto &p : prefixes) {
					                                        if (IsWithinPrefix(k, p)) {
						                                        return true;
					                                        }
				                                        }
				                                        return false;
			                                        }),
			                         result->point_keys.end());
		}

		// A range is only ever extracted from a top-level (AND-ed) filter, so it is AND-combined with
		// the whole points/prefixes group. If any point or prefix narrowing exists, the range is
		// therefore redundant: that scan already emits a superset of the matches and the post-filter
		// still enforces the bound. Drop the range so a row covered by both is never emitted twice.
		if (!result->point_keys.empty() || !result->prefixes.empty()) {
			result->ranges.clear();
		}
	}

	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> LevelPivotInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                               GlobalTableFunctionState *global_state) {
	auto lstate = make_uniq<LevelPivotScanLocalState>();
	auto &bind_data = input.bind_data->Cast<LevelPivotScanData>();
	auto &lp_table = bind_data.table_entry->Cast<LevelPivotTableEntry>();
	auto &catalog = lp_table.ParentCatalog().Cast<LevelPivotCatalog>();
	auto &txn = Transaction::Get(context.client, catalog).Cast<LevelPivotTransaction>();
	lstate->overlay = &txn.Overlay();
	return lstate;
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
		lstate.iterator = std::make_unique<level_pivot::MergedIterator>(connection.iterator(), lstate.overlay);
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
		gstate.done = true;
	}

	output.SetCardinality(count);
}

// True if `key` is at or below `range`'s upper bound (i.e. not yet past the end of the range).
static inline bool WithinUpper(std::string_view key, const RawKeyRange &range) {
	if (!range.has_hi) {
		return true;
	}
	int cmp = key.compare(std::string_view(range.hi));
	return range.hi_inclusive ? cmp <= 0 : cmp < 0;
}

static void RawScan(LevelPivotTableEntry &table_entry, LevelPivotScanLocalState &lstate,
                    LevelPivotScanGlobalState &gstate, DataChunk &output, const vector<column_t> &column_ids) {
	auto &connection = *table_entry.GetConnection();
	auto &columns = table_entry.GetColumns();
	auto &key_col_type = columns.GetColumn(LogicalIndex(0)).Type();
	auto &val_col_type = columns.GetColumn(LogicalIndex(1)).Type();
	bool val_is_json = table_entry.IsJsonColumn(1);

	const bool have_prefixes = !gstate.prefixes.empty();
	const bool have_ranges = !gstate.ranges.empty();
	const bool have_points = !gstate.point_keys.empty();

	if (!lstate.initialized) {
		lstate.initialized = true;
		// The prefix/range range-seeks and the unfiltered full scan need the iterator; point lookups
		// use direct gets. So create it unless this is a pure point lookup.
		if (have_prefixes || have_ranges || !have_points) {
			lstate.iterator = std::make_unique<level_pivot::MergedIterator>(connection.iterator(), lstate.overlay);
			if (!have_prefixes && !have_ranges) {
				lstate.iterator->seek_to_first(); // full scan; the range-seeks below seek per prefix/range
			}
		}
	}

	idx_t count = 0;

	// Emit one raw (key, value) row at the current `count` position.
	auto emit_row = [&](std::string_view key_sv, std::string_view val_sv) {
		for (idx_t i = 0; i < column_ids.size(); i++) {
			auto col_idx = column_ids[i];
			if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
				continue;
			}
			if (col_idx >= LEVEL_PIVOT_VIRTUAL_COL_BASE) {
				col_idx = col_idx - LEVEL_PIVOT_VIRTUAL_COL_BASE;
			}
			if (col_idx == 0) {
				WriteValueDirect(output.data[i], count, key_sv, key_col_type);
			} else if (col_idx == 1) {
				WriteValueDirect(output.data[i], count, val_sv, val_col_type, val_is_json);
			}
		}
	};

	// Point-lookup mode: bypass the iterator. Exact keys not covered by any prefix.
	if (have_points && lstate.point_key_index < gstate.point_keys.size()) {
		idx_t examined = 0;
		while (count < STANDARD_VECTOR_SIZE && lstate.point_key_index < gstate.point_keys.size()) {
			const std::string &key = gstate.point_keys[lstate.point_key_index++];

			std::string_view value_sv;
			std::string value_storage; // backs value_sv when reading from LevelDB
			bool found = false;
			if (lstate.overlay) {
				auto kind = lstate.overlay->lookup(key, &value_sv);
				if (kind == level_pivot::TransactionOverlay::Lookup::kPut) {
					found = true;
				} else if (kind == level_pivot::TransactionOverlay::Lookup::kTombstone) {
					continue; // deleted in this txn → not visible
				}
			}
			if (!found) {
				auto opt = connection.get(key);
				if (!opt) {
					continue;
				}
				value_storage = std::move(*opt);
				value_sv = std::string_view(value_storage.data(), value_storage.size());
			}
			examined++; // count only keys that resolved to a live row
			emit_row(std::string_view(key), value_sv);
			count++;
		}
		connection.note_rows_scanned(examined); // a point lookup examines one stored row per hit

		if (count >= STANDARD_VECTOR_SIZE) {
			output.SetCardinality(count);
			return; // resume point lookups (then prefixes) on the next call
		}
		// point keys exhausted with room left: fall through to the prefix scan below
	}

	// Prefix range-seek: prefixes are sorted and pairwise disjoint (see InitGlobal), so a single
	// forward pass seeks each prefix and scans only its subtree (+1 boundary row to detect the end).
	// IsWithinPrefix is a plain byte-prefix test, so the seeked range is exactly the set of keys that
	// `starts_with(key, pfx)` matches -- never a superset. (A caller wanting subtree-only semantics,
	// e.g. excluding the sibling 'a###bextra' from 'a###b', includes the trailing separator in pfx;
	// exactness against the original filter is in any case still enforced by DuckDB's post-filter.)
	if (have_prefixes) {
		idx_t examined = 0;
		while (lstate.prefix_index < gstate.prefixes.size() && count < STANDARD_VECTOR_SIZE) {
			const std::string &pfx = gstate.prefixes[lstate.prefix_index];
			if (!lstate.prefix_seeked) {
				lstate.iterator->seek(pfx);
				lstate.prefix_seeked = true;
			}
			while (count < STANDARD_VECTOR_SIZE && lstate.iterator->valid()) {
				std::string_view key_sv = lstate.iterator->key_view();
				examined++;
				if (!IsWithinPrefix(key_sv, pfx)) {
					break; // past this prefix's block (keys are sorted)
				}
				emit_row(key_sv, lstate.iterator->value_view());
				count++;
				lstate.iterator->next();
			}
			// Advance to the next prefix unless the chunk filled mid-block (resume there next call).
			if (!lstate.iterator->valid() || !IsWithinPrefix(lstate.iterator->key_view(), pfx)) {
				lstate.prefix_index++;
				lstate.prefix_seeked = false;
			}
		}
		connection.note_rows_scanned(examined);
		if (count >= STANDARD_VECTOR_SIZE) {
			output.SetCardinality(count);
			return; // resume prefixes (then ranges) on the next call
		}
		// prefixes exhausted with room left: fall through to the range scan below
	}

	// Bounded range-seek: the pushdown folds every key bound into one range (and InitGlobal drops it
	// entirely when a point/prefix scan is also present), so this loop normally runs a single range.
	// It seeks the lower bound and scans up to the upper bound (+1 boundary row to detect the end).
	if (have_ranges) {
		idx_t examined = 0;
		while (lstate.range_index < gstate.ranges.size() && count < STANDARD_VECTOR_SIZE) {
			const RawKeyRange &rng = gstate.ranges[lstate.range_index];
			if (!lstate.range_seeked) {
				if (rng.has_lo) {
					lstate.iterator->seek(rng.lo);
				} else {
					lstate.iterator->seek_to_first();
				}
				// `key > lo` (exclusive lower): the seek lands on lo itself, so skip it once here.
				// Only the first post-seek key can equal lo (keys are unique), so this stays out of
				// the per-row loop below.
				if (rng.has_lo && !rng.lo_inclusive && lstate.iterator->valid() &&
				    lstate.iterator->key_view() == std::string_view(rng.lo)) {
					lstate.iterator->next();
				}
				lstate.range_seeked = true;
			}
			while (count < STANDARD_VECTOR_SIZE && lstate.iterator->valid()) {
				std::string_view key_sv = lstate.iterator->key_view();
				examined++;
				if (!WithinUpper(key_sv, rng)) {
					break; // past this range's upper bound (keys are sorted)
				}
				emit_row(key_sv, lstate.iterator->value_view());
				count++;
				lstate.iterator->next();
			}
			// Advance to the next range unless the chunk filled mid-range (resume there next call).
			if (!lstate.iterator->valid() || !WithinUpper(lstate.iterator->key_view(), rng)) {
				lstate.range_index++;
				lstate.range_seeked = false;
			}
		}
		connection.note_rows_scanned(examined);
		if (count >= STANDARD_VECTOR_SIZE) {
			output.SetCardinality(count);
			return; // resume ranges on the next call
		}
	}

	// All pushdown narrowing (points / prefixes / ranges) is exhausted → done.
	if (have_points || have_prefixes || have_ranges) {
		gstate.done = true;
		output.SetCardinality(count);
		return;
	}

	// Unfiltered full scan (no pushdown predicates).
	while (count < STANDARD_VECTOR_SIZE && lstate.iterator && lstate.iterator->valid()) {
		emit_row(lstate.iterator->key_view(), lstate.iterator->value_view());
		count++;
		lstate.iterator->next();
	}
	connection.note_rows_scanned(count); // a full scan examines every row it emits

	if (!lstate.iterator || !lstate.iterator->valid()) {
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
