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

#include "duckdb/main/config.hpp"

namespace duckdb {

// Forward declarations for functions
TableFunction GetCreateTableFunction();
TableFunction GetDropTableFunction();
TableFunction GetDirtyTablesFunction();
TableFunction GetCheckIntegrityFunction();
TableFunction GetRepairStoreFunction();

static unique_ptr<Catalog> LevelPivotAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                            AttachedDatabase &db, const string &name, AttachInfo &info,
                                            AttachOptions &options) {
	// Parse options
	level_pivot::ConnectionOptions conn_opts;
	conn_opts.db_path = info.path;
	// Checked on every attach: the damage it looks for makes reads lose data
	// silently, so not looking is the expensive option.
	bool verify_tables = true;

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
		} else if (key == "verify_tables") {
			verify_tables = kv.second.GetValue<bool>();
		}
	}

	// A store whose tables hold keys out of order is only partly readable, and
	// leveldb reports no error for it: reads silently return less than is on
	// disk. Checking before the store is opened is the only point at which that
	// can be caught before a caller acts on a short read. One pass over the
	// tables: ~30ms for a 24MB store of 309k keys.
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
				               "' has problems that make reads return less than is stored: " + summary);
			} else {
				throw IOException("state store '" + info.path +
				                  "' has problems that make reads return less than is stored. "
				                  "Rebuild it with level_pivot_repair_store(). " +
				                  summary);
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
