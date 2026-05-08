#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "yyjson.hpp"
#include <string>
#include <string_view>
#include <vector>

namespace duckdb {

inline Value StringToTypedValue(std::string_view str_value, const LogicalType &type) {
	if (type.id() == LogicalTypeId::VARCHAR) {
		return Value(std::string(str_value));
	}
	return Value(std::string(str_value)).DefaultCastAs(type);
}

// Parse a JSON-encoded LevelDB value into a typed DuckDB Value.
// JSON null → DuckDB NULL. JSON string → extracted string. JSON number → cast to target type.
// Falls back to StringToTypedValue if JSON parse fails.
inline Value JsonStringToTypedValue(std::string_view str_value, const LogicalType &type) {
	auto *doc = duckdb_yyjson::yyjson_read(str_value.data(), str_value.size(), 0);
	if (!doc) {
		return StringToTypedValue(str_value, type);
	}

	auto *root = duckdb_yyjson::yyjson_doc_get_root(doc);
	Value result;

	if (duckdb_yyjson::yyjson_is_null(root)) {
		result = Value(type);
	} else if (type.id() == LogicalTypeId::VARCHAR) {
		if (duckdb_yyjson::yyjson_is_str(root)) {
			auto *s = duckdb_yyjson::yyjson_get_str(root);
			auto len = duckdb_yyjson::yyjson_get_len(root);
			result = Value(std::string(s, len));
		} else {
			size_t json_len = 0;
			char *json_str = duckdb_yyjson::yyjson_val_write(root, 0, &json_len);
			if (json_str) {
				result = Value(std::string(json_str, json_len));
				free(json_str);
			} else {
				result = Value(std::string(str_value));
			}
		}
	} else if (duckdb_yyjson::yyjson_is_bool(root)) {
		result = Value(std::string(duckdb_yyjson::yyjson_get_bool(root) ? "true" : "false")).DefaultCastAs(type);
	} else if (duckdb_yyjson::yyjson_is_num(root)) {
		if (duckdb_yyjson::yyjson_is_real(root)) {
			result = Value(std::to_string(duckdb_yyjson::yyjson_get_real(root))).DefaultCastAs(type);
		} else {
			result = Value(std::to_string(duckdb_yyjson::yyjson_get_sint(root))).DefaultCastAs(type);
		}
	} else {
		duckdb_yyjson::yyjson_doc_free(doc);
		return StringToTypedValue(str_value, type);
	}

	duckdb_yyjson::yyjson_doc_free(doc);
	return result;
}

// Escapes a UTF-8 string into a JSON string literal: prepends ", appends ", and
// escapes the JSON-mandatory characters and C0 control characters.
inline void AppendJsonString(std::string &out, std::string_view s) {
	out.push_back('"');
	for (char c : s) {
		switch (c) {
		case '"':
			out.append("\\\"");
			break;
		case '\\':
			out.append("\\\\");
			break;
		case '\b':
			out.append("\\b");
			break;
		case '\f':
			out.append("\\f");
			break;
		case '\n':
			out.append("\\n");
			break;
		case '\r':
			out.append("\\r");
			break;
		case '\t':
			out.append("\\t");
			break;
		default:
			if (static_cast<unsigned char>(c) < 0x20) {
				char buf[8];
				snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(c));
				out.append(buf);
			} else {
				out.push_back(c);
			}
		}
	}
	out.push_back('"');
}

// Serialize a DuckDB Value into a JSON-encoded string for LevelDB storage.
// VARCHAR values get JSON string quoting/escaping. Numeric/boolean types use ToString() (already valid JSON).
inline std::string TypedValueToJsonString(const Value &val, const LogicalType &type) {
	if (type.id() == LogicalTypeId::VARCHAR) {
		auto str = val.ToString();
		std::string out;
		out.reserve(str.size() + 2);
		AppendJsonString(out, str);
		return out;
	}

	return val.ToString();
}

inline bool IsWithinPrefix(std::string_view key, std::string_view prefix) {
	if (prefix.empty()) {
		return true;
	}
	if (key.size() < prefix.size()) {
		return false;
	}
	return key.compare(0, prefix.size(), prefix.data(), prefix.size()) == 0;
}

inline bool IdentityMatches(const std::vector<std::string> &identity, const std::string_view *captures, size_t count) {
	if (identity.size() != count) {
		return false;
	}
	for (size_t i = 0; i < count; ++i) {
		if (identity[i] != captures[i]) {
			return false;
		}
	}
	return true;
}

inline void ExtractIdentityValues(std::vector<std::string> &out, DataChunk &chunk, idx_t row, idx_t col_offset,
                                  idx_t num_cols) {
	out.clear();
	for (idx_t i = 0; i < num_cols; i++) {
		auto val = chunk.data[col_offset + i].GetValue(row);
		out.push_back(val.IsNull() ? "" : val.ToString());
	}
}

} // namespace duckdb
