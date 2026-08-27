#pragma once

#include <functional>
#include <string>
#include <vector>

#include <leveldb/slice.h>
#include <leveldb/status.h>

namespace level_pivot {

// Reads a store's own files - table files and write-ahead logs - one at a
// time, in the order each file stores its entries.
//
// This is deliberately not a DB iterator. leveldb's iterators assume every
// table is sorted and skip anything that sorts below what they already
// emitted, so on a store whose tables were written out of order they report
// less than is on disk: which is what has to be observed to detect the damage,
// and recovered to undo it. See Nexus-Mods/Vortex#23981.
//
// Reads only, so a store can be examined without taking its lock or running
// recovery over it.

// An entry as stored. The slices point into the reader's buffers and are valid
// only for the duration of the callback.
struct TableEntryView {
	leveldb::Slice user_key;
	uint64_t sequence = 0;
	// false for a deletion (a tombstone), whose value is empty
	bool is_value = false;
	leveldb::Slice value;
};

using TableEntryCallback = std::function<void(const TableEntryView &)>;

struct ReadStats {
	uint64_t entries = 0;
	// entries whose internal key or record framing could not be decoded; these
	// are skipped, and a caller repairing a store must not treat a non-zero
	// count as a clean recovery
	uint64_t unparsable = 0;
};

// Which of a store's files hold its current contents, as leveldb itself
// decides: the descriptor names the live tables, and only logs at or after the
// log number it records still matter.
//
// The distinction is not cosmetic. A table left behind by a compaction is
// still on disk and still full of readable keys, including keys whose
// deletions were dropped once they reached the base level. Reading it back
// would resurrect data the user deleted.
struct StoreFiles {
	std::vector<std::string> tables;
	std::vector<std::string> logs;
	// present on disk but not part of the current version: reported so a
	// caller can say so, never read
	std::vector<std::string> unreferenced;
	// named by the descriptor but absent from disk. leveldb refuses to open a
	// store in this state; anything rebuilding one has to treat it the same
	// way, or it hands back a store quietly missing whatever they held.
	std::vector<std::string> missing;
	// empty when the descriptor was read; otherwise why it could not be
	std::string descriptor_error;
};

// Resolves `db_dir`'s live files through leveldb's own descriptor recovery.
StoreFiles ListStoreFiles(const std::string &db_dir);

// Whether `db_dir` looks like a leveldb store at all - every store names its
// descriptor in CURRENT.
bool LooksLikeStore(const std::string &db_dir);

// Invokes `callback` for each entry of the table at `table_path`, verifying
// block checksums. A file that cannot be parsed - a partially written
// compaction output, say - returns a non-ok status.
leveldb::Status ReadTableEntries(const std::string &table_path,
                                 const TableEntryCallback &callback, ReadStats &stats);

// Invokes `callback` for each entry of the write-ahead log at `log_path`,
// verifying record checksums. Damaged records are counted as unparsable and
// skipped, matching leveldb's own recovery, which reports and continues.
leveldb::Status ReadLogEntries(const std::string &log_path, const TableEntryCallback &callback,
                               ReadStats &stats);

} // namespace level_pivot
