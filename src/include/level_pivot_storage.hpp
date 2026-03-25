#pragma once

#include <string>
#include <string_view>
#include <memory>
#include <optional>
#include <stdexcept>

namespace leveldb {
class DB;
class Iterator;
class WriteBatch;
} // namespace leveldb

namespace level_pivot {

class LevelDBError : public std::runtime_error {
public:
	explicit LevelDBError(const std::string &msg) : std::runtime_error(msg) {
	}
};

struct ConnectionOptions {
	std::string db_path;
	bool read_only = true;
	bool create_if_missing = false;
	size_t block_cache_size = static_cast<size_t>(8) * 1024 * 1024;
	size_t write_buffer_size = static_cast<size_t>(4) * 1024 * 1024;
};

class LevelDBIterator {
public:
	explicit LevelDBIterator(leveldb::DB *db);
	~LevelDBIterator();

	LevelDBIterator(LevelDBIterator &&other) noexcept;
	LevelDBIterator &operator=(LevelDBIterator &&other) noexcept;
	LevelDBIterator(const LevelDBIterator &) = delete;
	LevelDBIterator &operator=(const LevelDBIterator &) = delete;

	void seek(std::string_view key);
	void seek_to_first();
	void next();
	bool valid() const;
	std::string key() const;
	std::string value() const;
	std::string_view key_view() const;
	std::string_view value_view() const;

private:
	std::unique_ptr<leveldb::Iterator> iter_;
};

class LevelDBConnection;

class LevelDBWriteBatch {
public:
	explicit LevelDBWriteBatch(LevelDBConnection *connection);
	~LevelDBWriteBatch();

	LevelDBWriteBatch(LevelDBWriteBatch &&other) noexcept;
	LevelDBWriteBatch &operator=(LevelDBWriteBatch &&other) noexcept;
	LevelDBWriteBatch(const LevelDBWriteBatch &) = delete;
	LevelDBWriteBatch &operator=(const LevelDBWriteBatch &) = delete;

	void put(std::string_view key, std::string_view value);
	void del(std::string_view key);
	void commit();
	void discard();
	size_t pending_count() const;
	bool has_pending() const;

private:
	LevelDBConnection *connection_;
	std::unique_ptr<leveldb::WriteBatch> batch_;
	size_t pending_count_ = 0;
	bool committed_ = false;
};

class LevelDBConnection {
public:
	explicit LevelDBConnection(const ConnectionOptions &options);
	~LevelDBConnection();

	LevelDBConnection(const LevelDBConnection &) = delete;
	LevelDBConnection &operator=(const LevelDBConnection &) = delete;

	std::optional<std::string> get(std::string_view key);
	void put(std::string_view key, std::string_view value);
	void del(std::string_view key);
	LevelDBIterator iterator();
	LevelDBWriteBatch create_batch();

	const std::string &path() const {
		return path_;
	}
	bool is_read_only() const {
		return read_only_;
	}
	leveldb::DB *raw() {
		return db_;
	}

private:
	leveldb::DB *db_ = nullptr;
	std::string path_;
	bool read_only_;

	void check_write_allowed();
};

} // namespace level_pivot
