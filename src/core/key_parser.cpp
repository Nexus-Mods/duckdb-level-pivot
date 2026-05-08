#include "key_parser.hpp"
#include <stdexcept>

namespace level_pivot {

KeyParser::KeyParser(const KeyPattern &pattern) : pattern_(pattern) {
	compute_estimated_key_size();
	// After AttrSegment, capture all literal text that follows for fast key construction.
	attr_tail_.clear();
	bool past_attr = false;
	for (const auto &segment : pattern_.segments()) {
		if (past_attr) {
			if (std::holds_alternative<LiteralSegment>(segment)) {
				attr_tail_ += std::get<LiteralSegment>(segment).text;
			}
			// CaptureSegment after AttrSegment is unsupported; intentionally not handled.
		} else if (std::holds_alternative<AttrSegment>(segment)) {
			past_attr = true;
		}
	}
	try_init_simd_parser();
}

KeyParser::KeyParser(const std::string &pattern) : pattern_(pattern) {
	compute_estimated_key_size();
	// After AttrSegment, capture all literal text that follows for fast key construction.
	attr_tail_.clear();
	bool past_attr = false;
	for (const auto &segment : pattern_.segments()) {
		if (past_attr) {
			if (std::holds_alternative<LiteralSegment>(segment)) {
				attr_tail_ += std::get<LiteralSegment>(segment).text;
			}
		} else if (std::holds_alternative<AttrSegment>(segment)) {
			past_attr = true;
		}
	}
	try_init_simd_parser();
}

void KeyParser::compute_estimated_key_size() {
	constexpr size_t avg_capture_len = 16;
	estimated_key_size_ = 0;
	for (const auto &segment : pattern_.segments()) {
		if (std::holds_alternative<LiteralSegment>(segment)) {
			estimated_key_size_ += std::get<LiteralSegment>(segment).text.size();
		} else {
			estimated_key_size_ += avg_capture_len;
		}
	}
}

namespace {

template <typename ResultType>
struct ParsePolicy;

template <>
struct ParsePolicy<ParsedKeyView> {
	static void add_capture(ParsedKeyView &result, std::string_view key, size_t pos, size_t len) {
		result.capture_values.push_back(key.substr(pos, len));
	}
	static void set_attr(ParsedKeyView &result, std::string_view key, size_t pos, size_t len) {
		result.attr_name = key.substr(pos, len);
	}
};

template <typename ResultType>
std::optional<ResultType> parse_impl(const KeyPattern &pattern, std::string_view key) {
	const auto &segments = pattern.segments();
	ResultType result;
	result.capture_values.reserve(pattern.capture_count());

	size_t key_pos = 0;

	for (size_t seg_idx = 0; seg_idx < segments.size(); ++seg_idx) {
		const auto &segment = segments[seg_idx];

		if (std::holds_alternative<LiteralSegment>(segment)) {
			const auto &literal = std::get<LiteralSegment>(segment);
			if (key.compare(key_pos, literal.text.size(), literal.text) != 0) {
				return std::nullopt;
			}
			key_pos += literal.text.size();

		} else if (std::holds_alternative<CaptureSegment>(segment)) {
			size_t end_pos;
			if (seg_idx + 1 < segments.size()) {
				const auto &next_literal = std::get<LiteralSegment>(segments[seg_idx + 1]);
				end_pos = key.find(next_literal.text, key_pos);
				if (end_pos == std::string_view::npos) {
					return std::nullopt;
				}
			} else {
				end_pos = key.size();
			}

			if (end_pos == key_pos) {
				return std::nullopt;
			}

			ParsePolicy<ResultType>::add_capture(result, key, key_pos, end_pos - key_pos);
			key_pos = end_pos;

		} else if (std::holds_alternative<AttrSegment>(segment)) {
			size_t end_pos;
			if (seg_idx + 1 < segments.size()) {
				const auto &next_literal = std::get<LiteralSegment>(segments[seg_idx + 1]);
				end_pos = key.find(next_literal.text, key_pos);
				if (end_pos == std::string_view::npos) {
					return std::nullopt;
				}
			} else {
				end_pos = key.size();
			}

			if (end_pos == key_pos) {
				return std::nullopt;
			}

			ParsePolicy<ResultType>::set_attr(result, key, key_pos, end_pos - key_pos);
			key_pos = end_pos;
		}
	}

	if (key_pos != key.size()) {
		return std::nullopt;
	}

	return result;
}

} // anonymous namespace

std::optional<ParsedKeyView> KeyParser::parse_view(std::string_view key) const {
	if (simd_parser_) {
		std::string_view captures[MAX_KEY_CAPTURES];
		std::string_view attr;
		if (simd_parser_->parse_fast(key, captures, attr)) {
			ParsedKeyView result;
			result.capture_values.reserve(pattern_.capture_count());
			for (size_t i = 0; i < pattern_.capture_count(); ++i) {
				result.capture_values.push_back(captures[i]);
			}
			result.attr_name = attr;
			return result;
		}
		return std::nullopt;
	}
	return parse_impl<ParsedKeyView>(pattern_, key);
}

bool KeyParser::parse_fast(std::string_view key, std::string_view *captures, std::string_view &attr) const {
	if (simd_parser_) {
		return simd_parser_->parse_fast(key, captures, attr);
	}
	// Fallback: use generic parse_impl and copy results
	auto result = parse_impl<ParsedKeyView>(pattern_, key);
	if (!result) {
		return false;
	}
	for (size_t i = 0; i < result->capture_values.size(); ++i) {
		captures[i] = result->capture_values[i];
	}
	attr = result->attr_name;
	return true;
}

std::string KeyParser::build(const std::vector<std::string> &capture_values, const std::string &attr_name) const {
	if (capture_values.size() != pattern_.capture_count()) {
		throw std::invalid_argument("Expected " + std::to_string(pattern_.capture_count()) + " capture values, got " +
		                            std::to_string(capture_values.size()));
	}

	if (attr_name.empty()) {
		throw std::invalid_argument("attr_name cannot be empty");
	}

	std::string result;
	result.reserve(estimated_key_size_);
	size_t capture_idx = 0;

	for (const auto &segment : pattern_.segments()) {
		if (std::holds_alternative<LiteralSegment>(segment)) {
			result += std::get<LiteralSegment>(segment).text;
		} else if (std::holds_alternative<CaptureSegment>(segment)) {
			if (capture_values[capture_idx].empty()) {
				throw std::invalid_argument("Capture value for '" + std::get<CaptureSegment>(segment).name +
				                            "' cannot be empty");
			}
			result += capture_values[capture_idx];
			++capture_idx;
		} else if (std::holds_alternative<AttrSegment>(segment)) {
			result += attr_name;
		}
	}

	return result;
}

std::string KeyParser::build(const std::unordered_map<std::string, std::string> &captures,
                             const std::string &attr_name) const {
	std::vector<std::string> capture_values;
	capture_values.reserve(pattern_.capture_count());

	for (const auto &name : pattern_.capture_names()) {
		auto it = captures.find(name);
		if (it == captures.end()) {
			throw std::invalid_argument("Missing capture value for '" + name + "'");
		}
		capture_values.push_back(it->second);
	}

	return build(capture_values, attr_name);
}

std::string KeyParser::build_prefix() const {
	return pattern_.literal_prefix();
}

std::string KeyParser::build_prefix(const std::vector<std::string> &capture_values) const {
	std::string result;
	result.reserve(estimated_key_size_);
	size_t capture_idx = 0;

	for (const auto &segment : pattern_.segments()) {
		if (std::holds_alternative<LiteralSegment>(segment)) {
			result += std::get<LiteralSegment>(segment).text;
		} else if (std::holds_alternative<CaptureSegment>(segment)) {
			if (capture_idx >= capture_values.size()) {
				break;
			}
			result += capture_values[capture_idx];
			++capture_idx;
		} else if (std::holds_alternative<AttrSegment>(segment)) {
			break;
		}
	}

	return result;
}

void KeyParser::build_identity_prefix_with_delim_into(std::string &out,
                                                      const std::vector<std::string> &capture_values) const {
	if (capture_values.size() != pattern_.capture_count()) {
		throw std::invalid_argument("Expected " + std::to_string(pattern_.capture_count()) + " capture values, got " +
		                            std::to_string(capture_values.size()));
	}
	size_t capture_idx = 0;
	for (const auto &segment : pattern_.segments()) {
		if (std::holds_alternative<LiteralSegment>(segment)) {
			out += std::get<LiteralSegment>(segment).text;
		} else if (std::holds_alternative<CaptureSegment>(segment)) {
			if (capture_values[capture_idx].empty()) {
				throw std::invalid_argument("Capture value for '" + std::get<CaptureSegment>(segment).name +
				                            "' cannot be empty");
			}
			out += capture_values[capture_idx];
			++capture_idx;
		} else if (std::holds_alternative<AttrSegment>(segment)) {
			break; // stop here — caller appends attr name + attr_tail()
		}
	}
}

std::optional<std::string> KeyParser::try_get_uniform_delimiter() const {
	std::string delimiter;
	bool first_literal = true;

	for (size_t i = 0; i < pattern_.segments().size(); ++i) {
		const auto &seg = pattern_.segments()[i];

		if (std::holds_alternative<LiteralSegment>(seg)) {
			const auto &lit = std::get<LiteralSegment>(seg).text;

			// Skip the prefix - it's not a delimiter between captures
			if (first_literal && i == 0) {
				first_literal = false;
				continue;
			}

			if (delimiter.empty()) {
				delimiter = lit;
			} else if (delimiter != lit) {
				return std::nullopt;
			}
		} else {
			if (first_literal) {
				first_literal = false;
			}
		}
	}

	return delimiter.empty() ? std::nullopt : std::optional<std::string>(delimiter);
}

void KeyParser::try_init_simd_parser() {
	auto uniform_delim = try_get_uniform_delimiter();
	if (!uniform_delim) {
		return;
	}

	// Store owned copies - SIMD parser needs stable string_views
	simd_delimiter_ = *uniform_delim;
	simd_prefix_ = pattern_.literal_prefix();

	// The literal_prefix includes the trailing delimiter (e.g., "users##" for
	// pattern "users##{group}##..."). Strip it because the SIMD parser expects
	// the first delimiter to appear immediately after the prefix.
	if (simd_prefix_.size() >= simd_delimiter_.size() &&
	    simd_prefix_.compare(simd_prefix_.size() - simd_delimiter_.size(), simd_delimiter_.size(), simd_delimiter_) ==
	        0) {
		simd_prefix_.resize(simd_prefix_.size() - simd_delimiter_.size());
	}

	simd_parser_ = std::make_unique<SimdKeyParser>(simd_prefix_, simd_delimiter_, pattern_.capture_count());
}

} // namespace level_pivot
