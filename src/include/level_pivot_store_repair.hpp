#pragma once

#include <string>
#include <vector>

namespace level_pivot {

// Verifying and rebuilding a store whose table files hold keys out of order.
//
// leveldb resolves reads through the assumption that every table is sorted, so
// an unsorted table hides whole runs of live keys while the data sits intact
// on disk, and each compaction that reads it passes the disorder on. See
// Nexus-Mods/Vortex#23981.

struct StoreProblem {
	// table file the problem was found in
	std::string file;
	// "unsorted_keys", "unreadable_table" or "unparsable_entries"
	std::string problem;
	std::string detail;
};

struct RebuildStats {
	uint64_t files_read = 0;
	uint64_t entries_scanned = 0;
	uint64_t live_keys = 0;
	uint64_t suppressed_by_deletion = 0;
	uint64_t unparsable_entries = 0;
	// files on disk the descriptor does not reference, so not read; a
	// compaction interrupted part-way leaves these behind routinely
	uint64_t unreferenced_files = 0;
};

// Problems found in `db_dir`'s table files; empty means none were found. Reads
// only, so the store need not be closed and its lock is not taken. Costs one
// pass over the tables with checksums verified.
std::vector<StoreProblem> VerifyStore(const std::string &db_dir);

// Reads every file of `source` - tables and write-ahead logs - and writes the
// merged result into a new store at `target`, which must not exist. `source`
// is only read. Throws LevelDBError if anything cannot be read, rather than
// returning a store that is quietly missing keys.
RebuildStats RebuildStore(const std::string &source, const std::string &target);

} // namespace level_pivot
