#pragma once
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/function_set.hpp"
namespace duckdb {
TableFunctionSet GetStatsFunctionSet();
} // namespace duckdb
