#pragma once

#include "duckdb/function/table_function.hpp"
#include <vector>
#include <string>

namespace duckdb {

class LevelPivotTableEntry;

// Raw-mode bounded key range `[lo, hi]` (bounds optional, inclusivity per-end) extracted from
// `key >=/>/</<= const` and `BETWEEN` predicates. Scanned via a seek to `lo` and a walk up to `hi`.
struct RawKeyRange {
	string lo;                 // lower bound value (meaningful only if has_lo)
	string hi;                 // upper bound value (meaningful only if has_hi)
	bool has_lo = false;       // false = unbounded below (seek to first)
	bool has_hi = false;       // false = unbounded above (scan to end)
	bool lo_inclusive = true;  // key >= lo (true) vs key > lo (false)
	bool hi_inclusive = false; // key <= hi (true) vs key < hi (false)

	bool operator==(const RawKeyRange &o) const {
		return has_lo == o.has_lo && has_hi == o.has_hi && lo_inclusive == o.lo_inclusive &&
		       hi_inclusive == o.hi_inclusive && lo == o.lo && hi == o.hi;
	}
};

struct LevelPivotScanData : public TableFunctionData {
	LevelPivotTableEntry *table_entry;
	string filter_prefix;       // Narrowed prefix from pushdown_complex_filter (empty = use default)
	vector<string> point_keys;  // Raw-mode equality / IN-list pushdown
	vector<string> prefixes;    // Raw-mode starts_with(key, const) pushdown (subtree range-seek)
	vector<RawKeyRange> ranges; // Raw-mode comparison / BETWEEN pushdown (bounded range-seek)

	unique_ptr<FunctionData> Copy() const override {
		auto copy = make_uniq<LevelPivotScanData>();
		copy->table_entry = table_entry;
		copy->filter_prefix = filter_prefix;
		copy->point_keys = point_keys;
		copy->prefixes = prefixes;
		copy->ranges = ranges;
		return std::move(copy);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<LevelPivotScanData>();
		return table_entry == other.table_entry && filter_prefix == other.filter_prefix &&
		       point_keys == other.point_keys && prefixes == other.prefixes && ranges == other.ranges;
	}

	bool SupportStatementCache() const override {
		return false;
	}
};

struct LevelPivotScanGlobalState : public GlobalTableFunctionState {
	explicit LevelPivotScanGlobalState();
	idx_t MaxThreads() const override {
		return 1;
	}
	bool done = false;
	vector<column_t> column_ids;
	string filter_prefix;       // Narrowed prefix from filter pushdown (empty = use default)
	vector<string> point_keys;  // Copied from bind_data in InitGlobal
	vector<string> prefixes;    // Raw-mode starts_with prefixes (sorted, non-nested) for range-seek
	vector<RawKeyRange> ranges; // Raw-mode comparison / BETWEEN ranges for range-seek (≤1 after pushdown)
};

TableFunction LevelPivotScanFunction();

} // namespace duckdb
