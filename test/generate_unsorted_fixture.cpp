// Writes test/fixtures/unsorted_table: a leveldb store holding a table whose
// keys are not in order.
//
// This is the corruption seen in the field (Nexus-Mods/Vortex#23981). leveldb
// resolves reads through the assumption that every table is sorted, so such a
// table hides whole runs of live keys while the data sits intact on disk.
//
// Everything is written by leveldb's own writers - TableBuilder for the tables,
// VersionEdit and log::Writer for the descriptor, SetCurrentFile for CURRENT -
// so the fixture cannot drift from the format leveldb actually reads.
// Producing the unsorted table is what Options::check_key_order is for: this is
// the one caller that needs the guard off.
//
// Usage: generate_unsorted_fixture <output-dir>

#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include <leveldb/comparator.h>
#include <leveldb/env.h>
#include <leveldb/options.h>
#include <leveldb/status.h>
#include <leveldb/table_builder.h>

#include <db/dbformat.h>
#include <db/filename.h>
#include <db/log_writer.h>
#include <db/version_edit.h>

namespace {

// sequence numbers the entries carry, spaced so each group stays distinct
constexpr uint64_t kBaseSequence = 100;

struct Entry {
	std::string user_key;
	std::string value;
	uint64_t sequence;
};

leveldb::InternalKey KeyOf(const Entry &entry) {
	return leveldb::InternalKey(leveldb::Slice(entry.user_key), entry.sequence, leveldb::kTypeValue);
}

std::vector<Entry> Group(const char *key_prefix, uint64_t first_sequence, int count, const char *value_prefix) {
	std::vector<Entry> entries;
	for (int i = 0; i < count; i++) {
		char key[64];
		char value[64];
		std::snprintf(key, sizeof(key), "%s%02d", key_prefix, i);
		std::snprintf(value, sizeof(value), "%s%02d", value_prefix, i);
		entries.push_back({key, value, first_sequence + static_cast<uint64_t>(i)});
	}
	return entries;
}

void Fail(const std::string &what, const leveldb::Status &status) {
	std::fprintf(stderr, "%s: %s\n", what.c_str(), status.ToString().c_str());
	std::exit(1);
}

struct TableInfo {
	uint64_t size = 0;
	leveldb::InternalKey smallest;
	leveldb::InternalKey largest;
};

// Writes one table, each element of `blocks` becoming one data block.
// TableBuilder::Flush is the documented way to keep two entries out of the same
// block, which is what lets a table hold blocks that are each sorted but out of
// order relative to each other.
//
// smallest and largest are the first and last keys added, which is exactly what
// leveldb records - so on an unsorted table the recorded range inherits the
// disorder, as it did in the field.
TableInfo WriteTable(leveldb::Env *env, const std::string &path, const std::vector<std::vector<Entry>> &blocks) {
	leveldb::WritableFile *raw_file = nullptr;
	leveldb::Status status = env->NewWritableFile(path, &raw_file);
	if (!status.ok()) {
		Fail("cannot create " + path, status);
	}
	std::unique_ptr<leveldb::WritableFile> file(raw_file);

	leveldb::InternalKeyComparator icmp(leveldb::BytewiseComparator());
	leveldb::Options options;
	options.comparator = &icmp;
	options.check_key_order = false;

	TableInfo info;
	bool first = true;
	leveldb::TableBuilder builder(options, file.get());
	for (size_t block = 0; block < blocks.size(); block++) {
		for (auto &entry : blocks[block]) {
			const leveldb::InternalKey key = KeyOf(entry);
			if (first) {
				info.smallest = key;
				first = false;
			}
			info.largest = key;
			builder.Add(key.Encode(), leveldb::Slice(entry.value));
		}
		if (block + 1 < blocks.size()) {
			builder.Flush();
		}
	}

	status = builder.Finish();
	if (!status.ok()) {
		Fail("cannot finish " + path, status);
	}
	info.size = builder.FileSize();
	status = file->Close();
	if (!status.ok()) {
		Fail("cannot close " + path, status);
	}
	return info;
}

void WriteDescriptor(leveldb::Env *env, const std::string &dir, const TableInfo &unsorted, const TableInfo &sorted) {
	leveldb::VersionEdit edit;
	edit.SetComparatorName(leveldb::BytewiseComparator()->Name());
	edit.SetLogNumber(0);
	edit.SetNextFile(20);
	edit.SetLastSequence(kBaseSequence + 63);
	// both at level 0, the one level where leveldb allows overlapping ranges
	edit.AddFile(0, 5, unsorted.size, unsorted.smallest, unsorted.largest);
	edit.AddFile(0, 6, sorted.size, sorted.smallest, sorted.largest);

	const std::string name = leveldb::DescriptorFileName(dir, 1);
	leveldb::WritableFile *raw_file = nullptr;
	leveldb::Status status = env->NewWritableFile(name, &raw_file);
	if (!status.ok()) {
		Fail("cannot create " + name, status);
	}
	std::unique_ptr<leveldb::WritableFile> file(raw_file);

	std::string record;
	edit.EncodeTo(&record);
	leveldb::log::Writer writer(file.get());
	status = writer.AddRecord(leveldb::Slice(record));
	if (!status.ok()) {
		Fail("cannot write " + name, status);
	}
	status = file->Close();
	if (!status.ok()) {
		Fail("cannot close " + name, status);
	}

	// leveldb strips exactly one trailing newline from CURRENT, so letting it
	// write the file is what keeps a stray CR out of the descriptor's name
	status = leveldb::SetCurrentFile(env, dir, 1);
	if (!status.ok()) {
		Fail("cannot write CURRENT", status);
	}

	leveldb::FileLock *lock = nullptr;
	status = leveldb::Env::Default()->LockFile(leveldb::LockFileName(dir), &lock);
	if (!status.ok()) {
		Fail("cannot create LOCK", status);
	}
	leveldb::Env::Default()->UnlockFile(lock);
}

} // namespace

int main(int argc, char **argv) {
	if (argc < 2) {
		std::fprintf(stderr, "usage: generate_unsorted_fixture <output-dir>\n");
		return 2;
	}
	const std::string dir = argv[1];
	leveldb::Env *env = leveldb::Env::Default();
	env->CreateDir(dir);

	// Start from an empty directory: leftovers from a previous fixture, or from
	// anything that opened this one, would be picked up as part of the store.
	std::vector<std::string> existing;
	env->GetChildren(dir, &existing);
	for (auto &name : existing) {
		if (name != "." && name != ".." && name != ".gitattributes") {
			env->RemoveFile(dir + "/" + name);
		}
	}

	// 000005.ldb - two blocks, each sorted on its own, but the block holding the
	// higher keys is written first, and one pair is swapped inside the second
	// block. Both shapes were present in the field stores, and together they
	// give the two out-of-order transitions in 16 entries the tests assert on.
	std::vector<Entry> high = Group("key_c_", kBaseSequence, 8, "high-");
	std::vector<Entry> low = Group("key_a_", kBaseSequence + 20, 8, "low-");
	std::swap(low[3], low[4]);
	const TableInfo unsorted = WriteTable(env, dir + "/000005.ldb", {high, low});

	// 000006.ldb - sorted, so a report naming only 000005 shows the check reads
	// both and judges them apart.
	std::vector<Entry> ok = Group("key_z_", kBaseSequence + 40, 8, "ok-");
	const TableInfo sorted = WriteTable(env, dir + "/000006.ldb", {ok});

	// 000007.ldb - holds keys that were deleted, and is left out of the
	// descriptor. leveldb drops a tombstone once its key reaches the base level,
	// so a deletion leaves no trace beyond the key's absence from the live
	// tables. Anything that rebuilt a store by reading whatever .ldb files it
	// found would hand these back as if they had never been deleted.
	std::vector<Entry> obsolete = Group("key_deleted_", kBaseSequence + 60, 4, "resurrected-");
	WriteTable(env, dir + "/000007.ldb", {obsolete});

	WriteDescriptor(env, dir, unsorted, sorted);

	std::printf("wrote fixture to %s\n", dir.c_str());
	std::printf("  000005.ldb: blocks out of order, and one swapped pair within a block\n");
	std::printf("  000006.ldb: sorted\n");
	std::printf("  000007.ldb: sorted, but obsolete - not referenced by the descriptor\n");
	return 0;
}
