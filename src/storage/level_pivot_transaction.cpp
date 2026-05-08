#include "level_pivot_transaction.hpp"
#include "level_pivot_catalog.hpp"
#include "level_pivot_schema.hpp"
#include "level_pivot_table_entry.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

// --- LevelPivotTransaction ---

LevelPivotTransaction::LevelPivotTransaction(TransactionManager &manager, ClientContext &context,
                                             std::shared_ptr<level_pivot::LevelDBConnection> connection)
    : Transaction(manager, context), connection_(std::move(connection)) {
}

LevelPivotTransaction::~LevelPivotTransaction() = default;

void LevelPivotTransaction::CheckKeyAgainstTables(std::string_view key, LevelPivotSchemaEntry &schema) {
	if (all_dirty_) {
		return;
	}
	if (dirty_tables_.size() >= schema.TableCount()) {
		all_dirty_ = true;
		return;
	}

	schema.Scan(CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
		auto &table = entry.Cast<LevelPivotTableEntry>();
		const auto &table_name = table.name;

		// Skip tables already known dirty
		if (dirty_tables_.count(table_name)) {
			return;
		}

		if (table.GetTableMode() == LevelPivotTableMode::RAW) {
			// Raw tables are always affected by any write
			dirty_tables_.insert(table_name);
		} else {
			// Pivot table: fast prefix check, then full parse
			auto &parser = table.GetKeyParser();
			auto &prefix = parser.pattern().literal_prefix();
			if (!prefix.empty() && key.compare(0, prefix.size(), prefix) != 0) {
				return;
			}
			if (parser.parse_view(key).has_value()) {
				dirty_tables_.insert(table_name);
			}
		}
	});

	if (dirty_tables_.size() >= schema.TableCount()) {
		all_dirty_ = true;
	}
}

void LevelPivotTransaction::StagePut(std::string_view key, std::string_view value) {
	if (!pending_batch_) {
		pending_batch_ = std::make_unique<level_pivot::LevelDBWriteBatch>(connection_->create_batch());
	}
	pending_batch_->put(key, value);
	overlay_.put(key, value);
}

void LevelPivotTransaction::StageDelete(std::string_view key) {
	if (!pending_batch_) {
		pending_batch_ = std::make_unique<level_pivot::LevelDBWriteBatch>(connection_->create_batch());
	}
	pending_batch_->del(key);
	overlay_.del(key);
}

void LevelPivotTransaction::FlushPendingBatch() {
	if (pending_batch_) {
		pending_batch_->commit();
		pending_batch_.reset();
	}
	overlay_.clear();
}

void LevelPivotTransaction::DiscardPendingBatch() {
	if (pending_batch_) {
		pending_batch_->discard();
		pending_batch_.reset();
	}
	overlay_.clear();
}

// --- LevelPivotTransactionManager ---

LevelPivotTransactionManager::LevelPivotTransactionManager(AttachedDatabase &db) : TransactionManager(db) {
}

LevelPivotTransactionManager::~LevelPivotTransactionManager() = default;

Transaction &LevelPivotTransactionManager::StartTransaction(ClientContext &context) {
	lock_guard<mutex> l(transaction_lock);
	auto &lp_catalog = db.GetCatalog().Cast<LevelPivotCatalog>();
	current_transaction = make_uniq<LevelPivotTransaction>(*this, context, lp_catalog.GetConnection());
	return *current_transaction;
}

ErrorData LevelPivotTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	lock_guard<mutex> l(transaction_lock);
	auto &lp_txn = transaction.Cast<LevelPivotTransaction>();
	try {
		lp_txn.FlushPendingBatch();
	} catch (std::exception &e) {
		// Write failed — make sure we drop any partial state, then surface error.
		try {
			lp_txn.DiscardPendingBatch();
		} catch (...) {
			// best-effort; original error is what matters
		}
		current_transaction.reset();
		return ErrorData(e.what());
	}
	current_transaction.reset();
	return ErrorData();
}

void LevelPivotTransactionManager::RollbackTransaction(Transaction &transaction) {
	lock_guard<mutex> l(transaction_lock);
	auto &lp_txn = transaction.Cast<LevelPivotTransaction>();
	try {
		lp_txn.DiscardPendingBatch();
	} catch (...) {
		// nothing useful to do — discard should never throw, but keep this defensive
	}
	current_transaction.reset();
}

void LevelPivotTransactionManager::Checkpoint(ClientContext &context, bool force) {
}

LevelPivotTransaction *LevelPivotTransactionManager::GetCurrentTransaction() {
	lock_guard<mutex> l(transaction_lock);
	return current_transaction.get();
}

} // namespace duckdb
