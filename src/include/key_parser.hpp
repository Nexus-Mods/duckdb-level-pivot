#pragma once

#include "key_pattern.hpp"
#include "simd_parser.hpp"
#include <string>
#include <string_view>
#include <vector>
#include <optional>
#include <unordered_map>
#include <memory>

namespace level_pivot {

struct ParsedKeyView {
	std::vector<std::string_view> capture_values;
	std::string_view attr_name;

	bool operator==(const ParsedKeyView &other) const {
		return capture_values == other.capture_values && attr_name == other.attr_name;
	}
};

class KeyParser {
public:
	explicit KeyParser(const KeyPattern &pattern);
	explicit KeyParser(const std::string &pattern);

	const KeyPattern &pattern() const {
		return pattern_;
	}

	std::optional<ParsedKeyView> parse_view(std::string_view key) const;

	// Zero-alloc parse into pre-allocated buffers. Returns false if key doesn't match.
	// captures must point to an array with at least pattern().capture_count() elements.
	bool parse_fast(std::string_view key, std::string_view *captures, std::string_view &attr) const;

	std::string build(const std::vector<std::string> &capture_values, const std::string &attr_name) const;
	std::string build(const std::unordered_map<std::string, std::string> &captures, const std::string &attr_name) const;

	std::string build_prefix() const;
	std::string build_prefix(const std::vector<std::string> &capture_values) const;

	// Builds the prefix-up-to-attr-segment into `out` (assumed empty or to be cleared).
	// Returns nothing; caller appends attr_name + attr_tail() to complete the key.
	void build_identity_prefix_with_delim_into(std::string &out, const std::vector<std::string> &capture_values) const;

	// Returns the literal text immediately following the AttrSegment (often empty).
	const std::string &attr_tail() const {
		return attr_tail_;
	}

private:
	KeyPattern pattern_;
	size_t estimated_key_size_;
	std::string attr_tail_;

	// SIMD parser for uniform delimiter patterns (optional)
	std::unique_ptr<SimdKeyParser> simd_parser_;
	std::string simd_prefix_;
	std::string simd_delimiter_;

	void compute_estimated_key_size();
	void try_init_simd_parser();
	std::optional<std::string> try_get_uniform_delimiter() const;
};

} // namespace level_pivot
