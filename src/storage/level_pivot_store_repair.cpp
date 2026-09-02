#include "level_pivot_store_repair.hpp"

#include "level_pivot_storage.hpp"
#include "level_pivot_table_reader.hpp"

#include <map>
#include <string>

#include <leveldb/env.h>

namespace level_pivot {

namespace {

struct MergedEntry {
	uint64_t seq = 0;
	bool is_value = false;
	std::string value;
};

// Newest sequence wins per key, across every file and in whatever order the
// files are read: a deletion recorded later than a value suppresses it, and a
// value written later than a deletion brings the key back.
void Merge(const TableEntryView &view, std::map<std::string, MergedEntry> &best) {
	std::string user_key(view.user_key.data(), view.user_key.size());
	auto found = best.find(user_key);
	if (found != best.end() && found->second.seq >= view.sequence) {
		return;
	}
	MergedEntry entry;
	entry.seq = view.sequence;
	entry.is_value = view.is_value;
	if (view.is_value) {
		entry.value.assign(view.value.data(), view.value.size());
	}
	if (found == best.end()) {
		best.emplace(std::move(user_key), std::move(entry));
	} else {
		found->second = std::move(entry);
	}
}

void ReadStore(const std::string &source, std::map<std::string, MergedEntry> &best, RebuildStats &stats) {
	auto merge = [&best](const TableEntryView &view) {
		Merge(view, best);
	};

	// Only the files the descriptor says are live. Reading whatever else is
	// lying in the directory would bring back keys whose deletions leveldb
	// legitimately dropped once they reached the base level.
	const StoreFiles files = ListStoreFiles(source);
	if (!files.descriptor_error.empty()) {
		throw LevelDBError("cannot read the store's descriptor: " + files.descriptor_error);
	}
	if (!files.missing.empty()) {
		// Whatever those tables held cannot be recovered from anywhere else,
		// so a rebuild would be quietly short. leveldb refuses to open a store
		// in this state and so do we.
		throw LevelDBError("the store is missing " + std::to_string(files.missing.size()) +
		                   " table file(s) its descriptor requires, e.g. '" + files.missing[0] + "'");
	}
	stats.unreferenced_files = files.unreferenced.size();

	for (auto &name : files.tables) {
		ReadStats file_stats;
		auto status = ReadTableEntries(source + "/" + name, merge, file_stats);
		if (!status.ok()) {
			// A live table that will not parse means the rebuild is missing
			// keys, and saying so beats handing back a store that looks
			// complete.
			throw LevelDBError("cannot read table '" + name + "': " + status.ToString());
		}
		stats.files_read++;
		stats.entries_scanned += file_stats.entries;
		stats.unparsable_entries += file_stats.unparsable;
	}

	// A store closed with unflushed writes keeps them in its logs, so ignoring
	// those would silently drop the most recent changes.
	for (auto &name : files.logs) {
		ReadStats file_stats;
		auto status = ReadLogEntries(source + "/" + name, merge, file_stats);
		if (!status.ok()) {
			throw LevelDBError("cannot read write-ahead log '" + name + "': " + status.ToString());
		}
		stats.files_read++;
		stats.entries_scanned += file_stats.entries;
		stats.unparsable_entries += file_stats.unparsable;
	}
}

void WriteStore(const std::string &target, const std::map<std::string, MergedEntry> &best, RebuildStats &stats) {
	ConnectionOptions options;
	options.db_path = target;
	options.read_only = false;
	options.create_if_missing = true;
	LevelDBConnection connection(options);

	// commit() is one-shot, so each chunk gets a fresh batch
	auto batch = connection.create_batch();
	uint64_t batched = 0;
	for (auto &kv : best) {
		if (!kv.second.is_value) {
			stats.suppressed_by_deletion++;
			continue;
		}
		batch.put(kv.first, kv.second.value);
		stats.live_keys++;
		if (++batched >= 2000) {
			batch.commit();
			batch = connection.create_batch();
			batched = 0;
		}
	}
	// the last batch is synced, so a rebuild that reported success is on disk
	// rather than in the page cache when the caller acts on it
	batch.commit(true);
}

} // namespace

std::vector<StoreProblem> VerifyStore(const std::string &db_dir) {
	std::vector<StoreProblem> problems;
	const StoreFiles files = ListStoreFiles(db_dir);
	if (!files.descriptor_error.empty()) {
		problems.push_back({"CURRENT", "unreadable_descriptor", files.descriptor_error});
		return problems;
	}
	for (auto &name : files.missing) {
		problems.push_back({name, "missing_table", "named by the descriptor but not on disk"});
	}
	// A store closed with unflushed writes keeps them in its logs, so damage
	// there loses the most recent changes just as surely as a damaged table.
	for (auto &name : files.logs) {
		ReadStats stats;
		auto status = ReadLogEntries(
		    db_dir + "/" + name, [](const TableEntryView &) {}, stats);
		if (!status.ok()) {
			problems.push_back({name, "unreadable_log", status.ToString()});
		} else if (stats.unparsable > 0) {
			problems.push_back(
			    {name, "unparsable_entries", std::to_string(stats.unparsable) + " records could not be decoded"});
		}
	}
	for (auto &name : files.tables) {
		uint64_t transitions = 0;
		uint64_t entries = 0;
		uint64_t first_break_at = 0;
		std::string previous;
		uint64_t previous_sequence = 0;

		ReadStats stats;
		auto status = ReadTableEntries(
		    db_dir + "/" + name,
		    [&](const TableEntryView &entry) {
			    if (entries > 0) {
				    // InternalKeyComparator orders by user key ascending, then
				    // by sequence DESCENDING, so equal keys whose sequences
				    // climb are as out of order as a user-key regression and
				    // just as unreadable.
				    const int cmp = entry.user_key.compare(leveldb::Slice(previous));
				    if (cmp < 0 || (cmp == 0 && entry.sequence >= previous_sequence)) {
					    transitions++;
					    if (transitions == 1) {
						    first_break_at = entries;
					    }
				    }
			    }
			    previous.assign(entry.user_key.data(), entry.user_key.size());
			    previous_sequence = entry.sequence;
			    entries++;
		    },
		    stats);

		if (!status.ok()) {
			// A live table that cannot be parsed is a real problem, unlike the
			// half-written compaction outputs that ListStoreFiles has already
			// filtered out as unreferenced.
			problems.push_back({name, "unreadable_table", status.ToString()});
			continue;
		}
		if (transitions > 0) {
			problems.push_back({name, "unsorted_keys",
			                    std::to_string(transitions) + " out-of-order transitions in " +
			                        std::to_string(entries) + " entries; first break after entry " +
			                        std::to_string(first_break_at)});
		}
		if (stats.unparsable > 0) {
			problems.push_back(
			    {name, "unparsable_entries", std::to_string(stats.unparsable) + " entries could not be decoded"});
		}
	}
	return problems;
}

RebuildStats RebuildStore(const std::string &source, const std::string &target) {
	RebuildStats stats;
	std::map<std::string, MergedEntry> best;
	ReadStore(source, best, stats);
	if (best.empty()) {
		throw LevelDBError("no entries could be read from '" + source + "'");
	}
	WriteStore(target, best, stats);
	return stats;
}

} // namespace level_pivot
