#define DUCKDB_EXTENSION_MAIN

#include "level_pivot_extension.hpp"
#include "level_pivot_catalog.hpp"
#include "level_pivot_transaction.hpp"
#include "level_pivot_storage.hpp"
#include "level_pivot_stats.hpp"
#include "level_pivot_store_repair.hpp"
#include "level_pivot_table_reader.hpp"
#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/common/enums/access_mode.hpp"

#include <leveldb/env.h>
#include <leveldb/status.h>

#include <db/filename.h>
#include "duckdb/main/config.hpp"

namespace duckdb {

// Forward declarations for functions
TableFunction GetCreateTableFunction();
TableFunction GetDropTableFunction();
TableFunction GetDirtyTablesFunction();
TableFunction GetCheckIntegrityFunction();
TableFunction GetRepairStoreFunction();

// Rebuilds `db_path` in place: the replacement is assembled beside it, the
// original is kept under a `.corrupt-<n>` name as evidence, and only then does
// the replacement take the original's place. Nothing is deleted, so a swap
// that fails leaves both copies for a human to sort out.
static void RepairStoreInPlace(ClientContext &context, const string &db_path,
                               const string &summary) {
	auto &fs = FileSystem::GetFileSystem(context);

	// Both working names step aside if something is already there, so a
	// previous failure cannot make every later repair impossible.
	auto free_name = [&fs](const string &prefix) {
		for (idx_t attempt = 1;; attempt++) {
			const string candidate = prefix + to_string(attempt);
			if (!fs.DirectoryExists(candidate) && !fs.FileExists(candidate)) {
				return candidate;
			}
		}
	};
	const string preserved = free_name(db_path + ".corrupt-");
	const string rebuilt = free_name(db_path + ".rebuilt-");

	// The store must not be open anywhere else: this renames the directory out
	// from under whoever holds it, and on POSIX that succeeds while the other
	// process keeps writing into a directory nobody can reach any more.
	leveldb::FileLock *lock = nullptr;
	leveldb::Status lock_status =
	    leveldb::Env::Default()->LockFile(leveldb::LockFileName(db_path), &lock);
	if (!lock_status.ok()) {
		throw IOException("cannot repair '" + db_path +
		                  "' while it is open elsewhere: " + lock_status.ToString());
	}

	level_pivot::RebuildStats stats;
	auto discard = [&fs](const string &path) {
		try {
			if (fs.DirectoryExists(path)) {
				fs.RemoveDirectory(path);
			}
		} catch (...) {
			// the original failure is the one worth reporting
		}
	};

	try {
		stats = level_pivot::RebuildStore(db_path, rebuilt);
		if (stats.unparsable_entries > 0) {
			// The evidence copy is worthless once the user has been told the
			// repair was clean, so refuse rather than overstate it.
			throw IOException(
			    "rebuilding '" + db_path + "' could not decode " +
			    to_string(stats.unparsable_entries) +
			    " entries, so the result would be missing data; the store is left untouched and "
			    "the attempt is at '" + rebuilt + "'");
		}
		fs.MoveFile(db_path, preserved);
		try {
			fs.MoveFile(rebuilt, db_path);
		} catch (...) {
			try {
				// put the original back rather than leave no store at all
				fs.MoveFile(preserved, db_path);
			} catch (...) {
				// the rename that actually failed is the more useful error
			}
			throw;
		}
	} catch (...) {
		if (!fs.DirectoryExists(rebuilt) || fs.DirectoryExists(db_path)) {
			discard(rebuilt);
		}
		leveldb::Env::Default()->UnlockFile(lock);
		throw;
	}
	leveldb::Env::Default()->UnlockFile(lock);

	Printer::Print(StringUtil::Format(
	    "level_pivot: rebuilt state store '%s' (%llu keys from %llu entries across %llu files, "
	    "%llu unreferenced file(s) left alone); the original is kept at '%s'. Cause: %s",
	    db_path, stats.live_keys, stats.entries_scanned, stats.files_read,
	    stats.unreferenced_files, preserved, summary));
}

static unique_ptr<Catalog> LevelPivotAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &options) {
	// Parse options
	level_pivot::ConnectionOptions conn_opts;
	conn_opts.db_path = info.path;
	// Checked on every attach: the damage it looks for makes reads lose data
	// silently, so not looking is the expensive option. Rebuilding is not
	// automatic, because it rewrites the store - the caller opts in.
	bool verify_tables = true;
	bool repair_if_corrupt = false;

	// Access mode
	if (options.access_mode == AccessMode::READ_ONLY) {
		conn_opts.read_only = true;
	} else {
		conn_opts.read_only = false;
	}

	// Additional options from ATTACH statement
	for (auto &kv : info.options) {
		auto key = StringUtil::Lower(kv.first);
		if (key == "read_only") {
			conn_opts.read_only = kv.second.GetValue<bool>();
		} else if (key == "create_if_missing") {
			conn_opts.create_if_missing = kv.second.GetValue<bool>();
		} else if (key == "block_cache_size") {
			conn_opts.block_cache_size = kv.second.GetValue<int64_t>();
		} else if (key == "write_buffer_size") {
			conn_opts.write_buffer_size = kv.second.GetValue<int64_t>();
		} else if (key == "verify_checksums") {
			conn_opts.verify_checksums = kv.second.GetValue<bool>();
		} else if (key == "check_key_order") {
			conn_opts.check_key_order = kv.second.GetValue<bool>();
		} else if (key == "verify_tables") {
			verify_tables = kv.second.GetValue<bool>();
		} else if (key == "repair_if_corrupt") {
			repair_if_corrupt = kv.second.GetValue<bool>();
		}
	}

	// A store whose tables hold keys out of order is only partly readable, and
	// leveldb reports no error for it: reads silently return less than is on
	// disk. Checking before the store is opened is the only point at which
	// that can be caught, and the only point at which it can still be
	// rebuilt. One pass over the tables, ~80ms for a 20MB store.
	if (verify_tables && level_pivot::LooksLikeStore(info.path)) {
		auto problems = level_pivot::VerifyStore(info.path);
		if (!problems.empty()) {
			string summary;
			for (auto &problem : problems) {
				if (!summary.empty()) {
					summary += "; ";
				}
				summary += problem.file + ": " + problem.problem + " (" + problem.detail + ")";
			}
			if (conn_opts.read_only) {
				// A read-only attach is exactly where short reads go unnoticed,
				// so it is worth saying; but rebuilding needs write access and
				// the caller has asked for none.
				Printer::Print("level_pivot: state store '" + info.path +
				               "' has problems that make reads return less than is stored: " +
				               summary);
			} else if (!repair_if_corrupt) {
				throw IOException("state store '" + info.path +
				                  "' has problems that make reads return less than is stored. "
				                  "Attach with repair_if_corrupt to rebuild it. " +
				                  summary);
			} else {
				RepairStoreInPlace(context, info.path, summary);
			}
		}
	}

	// Open LevelDB
	auto connection = std::make_shared<level_pivot::LevelDBConnection>(conn_opts);

	return make_uniq<LevelPivotCatalog>(db, std::move(connection));
}

static unique_ptr<TransactionManager>
LevelPivotCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info, AttachedDatabase &db,
                                   Catalog &catalog) {
	return make_uniq<LevelPivotTransactionManager>(db);
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register storage extension
	auto storage_ext = make_uniq<StorageExtension>();
	storage_ext->attach = LevelPivotAttach;
	storage_ext->create_transaction_manager = LevelPivotCreateTransactionManager;
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	auto shared = shared_ptr<StorageExtension>(storage_ext.release());
	StorageExtension::Register(config, "level_pivot", std::move(shared));

	// Register utility table functions
	loader.RegisterFunction(GetCreateTableFunction());
	loader.RegisterFunction(GetDropTableFunction());
	loader.RegisterFunction(GetDirtyTablesFunction());
	loader.RegisterFunction(GetCheckIntegrityFunction());
	loader.RegisterFunction(GetRepairStoreFunction());
	loader.RegisterFunction(GetStatsFunctionSet());
}

void LevelPivotExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string LevelPivotExtension::Name() {
	return "level_pivot";
}

std::string LevelPivotExtension::Version() const {
#ifdef EXT_VERSION_LEVEL_PIVOT
	return EXT_VERSION_LEVEL_PIVOT;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(level_pivot, loader) {
	duckdb::LoadInternal(loader);
}
}
