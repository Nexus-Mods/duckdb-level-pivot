#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "level_pivot_overlay.hpp"
#include "level_pivot_storage.hpp"
#include <memory>
#include <unordered_set>
#include <string>
#include <string_view>

namespace duckdb {

class LevelPivotSchemaEntry;

class LevelPivotTransaction : public Transaction {
public:
	LevelPivotTransaction(TransactionManager &manager, ClientContext &context,
	                      std::shared_ptr<level_pivot::LevelDBConnection> connection);
	~LevelPivotTransaction() override;

	//! Check a key against all tables in the schema and mark matching ones dirty
	void CheckKeyAgainstTables(std::string_view key, LevelPivotSchemaEntry &schema);

	//! Mark a specific table dirty without scanning the schema
	void MarkTableDirty(const std::string &table_name) {
		dirty_tables_.insert(table_name);
	}

	bool HasDirtyTables() const {
		return !dirty_tables_.empty();
	}
	const std::unordered_set<std::string> &GetDirtyTables() const {
		return dirty_tables_;
	}

	//! Read-side overlay (used by MergedIterator in scans)
	level_pivot::TransactionOverlay &Overlay() {
		return overlay_;
	}
	const level_pivot::TransactionOverlay &Overlay() const {
		return overlay_;
	}

	//! Stage a put/delete against the pending batch + overlay. No LevelDB Write yet.
	void StagePut(std::string_view key, std::string_view value);
	void StageDelete(std::string_view key);

	//! Flush pending batch as one LevelDB Write; clears overlay. Throws on Write failure.
	void FlushPendingBatch();

	//! Discard pending batch + overlay. No-op if no writes were staged.
	void DiscardPendingBatch();

private:
	std::shared_ptr<level_pivot::LevelDBConnection> connection_;
	std::unordered_set<std::string> dirty_tables_;
	bool all_dirty_ = false;
	level_pivot::TransactionOverlay overlay_;
	// Lazily created on first stage; nullptr means no pending writes
	std::unique_ptr<level_pivot::LevelDBWriteBatch> pending_batch_;
};

class LevelPivotTransactionManager : public TransactionManager {
public:
	explicit LevelPivotTransactionManager(AttachedDatabase &db);
	~LevelPivotTransactionManager() override;

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force = false) override;

	//! Get the current transaction, or nullptr if none active
	LevelPivotTransaction *GetCurrentTransaction();

private:
	mutex transaction_lock;
	unique_ptr<LevelPivotTransaction> current_transaction;
};

} // namespace duckdb
