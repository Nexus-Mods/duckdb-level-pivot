#include "level_pivot_store_repair.hpp"
#include "level_pivot_table_reader.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

// Surfaces level_pivot::VerifyStore as SQL, so a suspect store can be
// examined without opening it.

namespace {

struct CheckIntegrityBindData : public TableFunctionData {
	vector<level_pivot::StoreProblem> rows;
	idx_t offset = 0;
};

unique_ptr<FunctionData> CheckIntegrityBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto data = make_uniq<CheckIntegrityBindData>();

	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("file");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("problem");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("detail");

	if (input.inputs[0].IsNull()) {
		throw BinderException("level_pivot_check_integrity requires a store path");
	}
	const string db_path = input.inputs[0].ToString();

	auto &fs = FileSystem::GetFileSystem(context);
	if (!fs.DirectoryExists(db_path)) {
		throw IOException("cannot read store directory '" + db_path + "'");
	}
	// A path that is merely a directory must fail loudly: reporting no
	// problems for it would read as a clean bill of health. A store with
	// nothing flushed yet has no table files and legitimately reports nothing.
	if (!level_pivot::LooksLikeStore(db_path)) {
		throw IOException("'" + db_path + "' is not a leveldb store (no CURRENT file)");
	}
	data->rows = level_pivot::VerifyStore(db_path);

	return std::move(data);
}

void CheckIntegrityFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<CheckIntegrityBindData>();

	idx_t count = 0;
	while (bind_data.offset < bind_data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = bind_data.rows[bind_data.offset];
		output.SetValue(0, count, Value(row.file));
		output.SetValue(1, count, Value(row.problem));
		output.SetValue(2, count, Value(row.detail));
		bind_data.offset++;
		count++;
	}
	output.SetCardinality(count);
}

} // namespace

TableFunction GetCheckIntegrityFunction() {
	TableFunction func("level_pivot_check_integrity", {LogicalType::VARCHAR}, CheckIntegrityFunc, CheckIntegrityBind);
	return func;
}

} // namespace duckdb
