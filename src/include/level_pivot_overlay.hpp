#pragma once

#include <map>
#include <optional>
#include <string>
#include <string_view>
#include "level_pivot_storage.hpp"

namespace level_pivot {

// In-memory log of pending writes for an active transaction.
// std::less<> enables transparent string_view lookup against std::string keys.
class TransactionOverlay {
public:
	void put(std::string_view key, std::string_view value);
	void del(std::string_view key);
	void clear();
	bool empty() const {
		return entries_.empty();
	}
	size_t size() const {
		return entries_.size();
	}

	enum class Lookup { kAbsent, kPut, kTombstone };
	// Returns kPut + sets *out_value when a put exists; kTombstone for a delete; kAbsent when unmapped.
	// out_value may be nullptr if the caller only cares about the tag.
	Lookup lookup(std::string_view key, std::string_view *out_value) const;

	// Iteration in key order. Each entry is (key, optional<value>) — nullopt = tombstone.
	using Map = std::map<std::string, std::optional<std::string>, std::less<>>;
	Map::const_iterator begin() const {
		return entries_.begin();
	}
	Map::const_iterator end() const {
		return entries_.end();
	}
	Map::const_iterator lower_bound(std::string_view key) const {
		return entries_.lower_bound(key);
	}

private:
	Map entries_;
};

// Iterates the union of a base LevelDB iterator and a TransactionOverlay
// in key-sorted order, hiding tombstones and preferring overlay over base
// when the same key is present in both.
class MergedIterator {
public:
	MergedIterator(LevelDBIterator base, const TransactionOverlay *overlay);

	void seek(std::string_view key);
	void seek_to_first();
	void next();
	bool valid() const {
		return state_ != State::kEnd;
	}
	std::string_view key_view() const;
	std::string_view value_view() const;

private:
	enum class State { kBase, kOverlay, kEnd };
	LevelDBIterator base_;
	const TransactionOverlay *overlay_; // may be null
	TransactionOverlay::Map::const_iterator ov_it_;
	State state_ = State::kEnd;

	void advance_to_next_visible();
	void choose_state();
};

} // namespace level_pivot
