#include "level_pivot_store_repair.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"

#include <leveldb/env.h>
#include <leveldb/status.h>

#include <db/filename.h>

namespace duckdb {

// Surfaces level_pivot::RebuildStore as SQL. The source is only read, and the
// result is assembled under a temporary name and renamed into place, so a
// failure part-way through cannot leave something that looks like a repaired
// store. The caller decides whether to swap source and target.

namespace {

struct RepairBindData : public TableFunctionData {
	level_pivot::RebuildStats stats;
	idx_t offset = 0;
};

unique_ptr<FunctionData> RepairBind(ClientContext &context, TableFunctionBindInput &input,
                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<RepairBindData>();

	return_types.push_back(LogicalType::UBIGINT);
	names.push_back("files_read");
	return_types.push_back(LogicalType::UBIGINT);
	names.push_back("entries_scanned");
	return_types.push_back(LogicalType::UBIGINT);
	names.push_back("live_keys");
	return_types.push_back(LogicalType::UBIGINT);
	names.push_back("suppressed_by_deletion");
	return_types.push_back(LogicalType::UBIGINT);
	names.push_back("unparsable_entries");
	return_types.push_back(LogicalType::UBIGINT);
	names.push_back("unreferenced_files");

	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("level_pivot_repair_store requires a source and a target path");
	}
	const string source = input.inputs[0].ToString();
	const string target = input.inputs[1].ToString();

	auto &fs = FileSystem::GetFileSystem(context);
	if (!fs.DirectoryExists(source)) {
		throw IOException("store to repair does not exist: '" + source + "'");
	}
	if (fs.DirectoryExists(target) || fs.FileExists(target)) {
		throw IOException("target '" + target + "' already exists; repair never overwrites");
	}
	// only the store directory itself is created, so a mistyped path fails
	// here rather than scattering directories
	const auto target_parent = StringUtil::GetFilePath(target);
	if (!target_parent.empty() && !fs.DirectoryExists(target_parent)) {
		throw IOException("target directory '" + target_parent + "' does not exist");
	}

	// Assembled under a temporary name: a store that stopped half way through
	// being written would open cleanly and look like a complete repair.
	const string pending = target + ".pending";
	if (fs.DirectoryExists(pending)) {
		throw IOException("'" + pending + "' is in the way of the repair; remove it first");
	}

	// The source must not be open anywhere else.
	leveldb::FileLock *lock = nullptr;
	const leveldb::Status lock_status = leveldb::Env::Default()->LockFile(leveldb::LockFileName(source), &lock);
	if (!lock_status.ok()) {
		throw IOException("cannot repair '" + source + "' while it is open elsewhere: " + lock_status.ToString());
	}

	try {
		data->stats = level_pivot::RebuildStore(source, pending);
	} catch (...) {
		try {
			if (fs.DirectoryExists(pending)) {
				fs.RemoveDirectory(pending);
			}
		} catch (...) {
			// the original failure is the one worth reporting; a leftover
			// pending directory is inert and named so it cannot be mistaken
			// for a repaired store
		}
		leveldb::Env::Default()->UnlockFile(lock);
		throw;
	}
	leveldb::Env::Default()->UnlockFile(lock);
	fs.MoveFile(pending, target);

	return std::move(data);
}

void RepairFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<RepairBindData>();
	if (bind_data.offset > 0) {
		output.SetCardinality(0);
		return;
	}
	auto &stats = bind_data.stats;
	output.SetValue(0, 0, Value::UBIGINT(stats.files_read));
	output.SetValue(1, 0, Value::UBIGINT(stats.entries_scanned));
	output.SetValue(2, 0, Value::UBIGINT(stats.live_keys));
	output.SetValue(3, 0, Value::UBIGINT(stats.suppressed_by_deletion));
	output.SetValue(4, 0, Value::UBIGINT(stats.unparsable_entries));
	output.SetValue(5, 0, Value::UBIGINT(stats.unreferenced_files));
	output.SetCardinality(1);
	bind_data.offset = 1;
}

} // namespace

TableFunction GetRepairStoreFunction() {
	TableFunction func("level_pivot_repair_store", {LogicalType::VARCHAR, LogicalType::VARCHAR}, RepairFunc,
	                   RepairBind);
	return func;
}

} // namespace duckdb
