#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

class LevelPivotTableEntry;

class LevelPivotInsert : public PhysicalOperator {
public:
	LevelPivotInsert(PhysicalPlan &plan, vector<LogicalType> types, TableCatalogEntry &table,
	                 idx_t estimated_cardinality);

	TableCatalogEntry &table;

	// --- Sink interface ---
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}

	// --- Source interface ---
	bool IsSource() const override {
		return true;
	}
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;
};

} // namespace duckdb
