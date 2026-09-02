#include "level_pivot_table_reader.hpp"

#include <algorithm>
#include <memory>
#include <set>

#include <leveldb/comparator.h>
#include <leveldb/env.h>
#include <leveldb/iterator.h>
#include <leveldb/options.h>
#include <leveldb/table.h>
#include <leveldb/write_batch.h>

#include <db/dbformat.h>
#include <db/filename.h>
#include <db/log_reader.h>
#include <db/table_cache.h>
#include <db/version_set.h>
#include <db/write_batch_internal.h>

namespace level_pivot {

namespace {

// Collects the entries of one WriteBatch. leveldb assigns each entry the
// batch's base sequence plus its index, which is what makes a log record's
// entries comparable with those already read from tables.
class BatchCollector : public leveldb::WriteBatch::Handler {
public:
	BatchCollector(uint64_t base_sequence, const TableEntryCallback &callback, ReadStats &stats)
	    : sequence_(base_sequence), callback_(callback), stats_(stats) {
	}

	void Put(const leveldb::Slice &key, const leveldb::Slice &value) override {
		TableEntryView entry;
		entry.user_key = key;
		entry.sequence = sequence_++;
		entry.is_value = true;
		entry.value = value;
		stats_.entries++;
		callback_(entry);
	}

	void Delete(const leveldb::Slice &key) override {
		TableEntryView entry;
		entry.user_key = key;
		entry.sequence = sequence_++;
		entry.is_value = false;
		stats_.entries++;
		callback_(entry);
	}

private:
	uint64_t sequence_;
	const TableEntryCallback &callback_;
	ReadStats &stats_;
};

// A damaged log record is dropped and counted, which is what leveldb's own
// recovery does: it reports the loss and carries on rather than discarding
// everything after it.
class LogReporter : public leveldb::log::Reader::Reporter {
public:
	explicit LogReporter(ReadStats &stats) : stats_(stats) {
	}

	void Corruption(size_t bytes, const leveldb::Status &status) override {
		(void)bytes;
		(void)status;
		stats_.unparsable++;
	}

private:
	ReadStats &stats_;
};

} // namespace

bool LooksLikeStore(const std::string &db_dir) {
	return leveldb::Env::Default()->FileExists(leveldb::CurrentFileName(db_dir));
}

StoreFiles ListStoreFiles(const std::string &db_dir) {
	StoreFiles result;
	leveldb::Env *env = leveldb::Env::Default();

	// leveldb's own descriptor recovery decides what is live. Reuse of the
	// descriptor is off by default, so this only reads.
	leveldb::Options options;
	options.paranoid_checks = true;
	leveldb::InternalKeyComparator icmp(leveldb::BytewiseComparator());
	// The cache hands its options straight to Table::Open, which would then
	// compare internal keys with the user comparator. Nothing here opens a
	// table through the cache, but leveldb substitutes the internal comparator
	// for exactly this reason and so should we.
	leveldb::Options cache_options = options;
	cache_options.comparator = &icmp;
	leveldb::TableCache table_cache(db_dir, cache_options, 16);
	leveldb::VersionSet versions(db_dir, &options, &table_cache, &icmp);

	bool save_manifest = false;
	leveldb::Status status = versions.Recover(&save_manifest);
	if (!status.ok()) {
		result.descriptor_error = status.ToString();
		return result;
	}

	std::set<uint64_t> live;
	versions.AddLiveFiles(&live);
	const uint64_t min_log = versions.LogNumber();
	const uint64_t prev_log = versions.PrevLogNumber();

	std::vector<std::string> children;
	if (!env->GetChildren(db_dir, &children).ok()) {
		result.descriptor_error = "cannot list the store directory";
		return result;
	}

	for (auto &name : children) {
		uint64_t number = 0;
		leveldb::FileType type;
		// names leveldb did not write are not the store's business
		if (!leveldb::ParseFileName(name, &number, &type)) {
			continue;
		}
		if (type == leveldb::kTableFile) {
			// erase as we go, so whatever the descriptor still expects
			// afterwards is a file that is not on disk
			if (live.erase(number) > 0) {
				result.tables.push_back(name);
			} else {
				result.unreferenced.push_back(name);
			}
		} else if (type == leveldb::kLogFile) {
			if (number >= min_log || number == prev_log) {
				result.logs.push_back(name);
			} else {
				result.unreferenced.push_back(name);
			}
		}
	}

	for (uint64_t number : live) {
		result.missing.push_back(leveldb::TableFileName(std::string(), number).substr(1));
	}

	// deterministic order, so a report reads the same way twice
	std::sort(result.tables.begin(), result.tables.end());
	std::sort(result.logs.begin(), result.logs.end());
	std::sort(result.unreferenced.begin(), result.unreferenced.end());
	std::sort(result.missing.begin(), result.missing.end());
	return result;
}

leveldb::Status ReadTableEntries(const std::string &table_path, const TableEntryCallback &callback, ReadStats &stats) {
	leveldb::Env *env = leveldb::Env::Default();
	uint64_t file_size = 0;
	leveldb::Status status = env->GetFileSize(table_path, &file_size);
	if (!status.ok()) {
		return status;
	}

	leveldb::RandomAccessFile *file = nullptr;
	status = env->NewRandomAccessFile(table_path, &file);
	if (!status.ok()) {
		return status;
	}
	// Table::Open does not take ownership, but the file has to outlive the
	// table: declared first here, so it is destroyed last.
	std::unique_ptr<leveldb::RandomAccessFile> file_guard(file);

	leveldb::Table *table = nullptr;
	leveldb::Options options;
	// a tool for reading damaged stores has to check what it reads
	options.paranoid_checks = true;
	status = leveldb::Table::Open(options, file, file_size, &table);
	if (!status.ok()) {
		return status;
	}
	std::unique_ptr<leveldb::Table> table_guard(table);

	leveldb::ReadOptions read_options;
	read_options.verify_checksums = true;
	// Only SeekToFirst/Next are used, so the default comparator in `options`
	// never orders anything; adding a Seek here would need the internal key
	// comparator instead.
	std::unique_ptr<leveldb::Iterator> iter(table->NewIterator(read_options));
	for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
		leveldb::ParsedInternalKey parsed;
		// upstream's parser rejects an unknown value type, so a damaged
		// trailer cannot be mistaken for a deletion and drop the key
		if (!leveldb::ParseInternalKey(iter->key(), &parsed)) {
			stats.unparsable++;
			continue;
		}
		TableEntryView entry;
		entry.user_key = parsed.user_key;
		entry.sequence = parsed.sequence;
		entry.is_value = parsed.type == leveldb::kTypeValue;
		if (entry.is_value) {
			entry.value = iter->value();
		}
		stats.entries++;
		callback(entry);
	}
	return iter->status();
}

leveldb::Status ReadLogEntries(const std::string &log_path, const TableEntryCallback &callback, ReadStats &stats) {
	leveldb::Env *env = leveldb::Env::Default();
	leveldb::SequentialFile *file = nullptr;
	leveldb::Status status = env->NewSequentialFile(log_path, &file);
	if (!status.ok()) {
		return status;
	}
	std::unique_ptr<leveldb::SequentialFile> file_guard(file);

	LogReporter reporter(stats);
	leveldb::log::Reader reader(file, &reporter, /*checksum=*/true, /*initial_offset=*/0);

	std::string scratch;
	leveldb::Slice record;
	while (reader.ReadRecord(&record, &scratch)) {
		if (record.size() < 12) {
			stats.unparsable++;
			continue;
		}
		leveldb::WriteBatch batch;
		leveldb::WriteBatchInternal::SetContents(&batch, record);
		BatchCollector collector(leveldb::WriteBatchInternal::Sequence(&batch), callback, stats);
		// Iterate checks the record's own count, so a truncated batch is
		// reported rather than silently dropping its tail
		if (!batch.Iterate(&collector).ok()) {
			stats.unparsable++;
		}
	}
	return leveldb::Status::OK();
}

} // namespace level_pivot
