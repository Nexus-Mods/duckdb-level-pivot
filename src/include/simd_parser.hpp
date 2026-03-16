#pragma once

#include <string_view>
#include <vector>
#include <optional>
#include <cstdint>
#include <cstring>

#if defined(__x86_64__) || defined(_M_X64)
#define LEVEL_PIVOT_X86_64 1
#include <immintrin.h>
#if defined(_MSC_VER)
#include <intrin.h>
#endif
#endif

#if defined(__aarch64__) || defined(_M_ARM64)
#define LEVEL_PIVOT_AARCH64 1
#include <arm_neon.h>
#endif

namespace level_pivot {

static constexpr size_t MAX_KEY_CAPTURES = 16;

// =============================================================================
// CPU Feature Detection (runs once, cached)
// =============================================================================

namespace detail {

struct CpuFeatures {
	bool has_sse2 = false;
	bool has_avx2 = false;
	bool has_neon = false;

	static const CpuFeatures &get() {
		static const CpuFeatures instance = detect();
		return instance;
	}

private:
	static CpuFeatures detect() {
		CpuFeatures f;
#if defined(LEVEL_PIVOT_X86_64)
#if defined(_MSC_VER)
		int cpuInfo[4];
		__cpuid(cpuInfo, 0);
		int nIds = cpuInfo[0];

		if (nIds >= 1) {
			__cpuid(cpuInfo, 1);
			f.has_sse2 = (cpuInfo[3] & (1 << 26)) != 0;
		}
		if (nIds >= 7) {
			__cpuidex(cpuInfo, 7, 0);
			f.has_avx2 = (cpuInfo[1] & (1 << 5)) != 0;
		}
#else
		// GCC/Clang
		__builtin_cpu_init();
		f.has_sse2 = __builtin_cpu_supports("sse2");
		f.has_avx2 = __builtin_cpu_supports("avx2");
#endif
#endif
#if defined(LEVEL_PIVOT_AARCH64)
		f.has_neon = true; // NEON is mandatory on ARMv8/aarch64
#endif
		return f;
	}
};

} // namespace detail

// =============================================================================
// SIMD Implementation Functions (with target attributes for runtime dispatch)
// =============================================================================

namespace detail {

// Scalar implementation (always available)
inline void find_delimiters_scalar(const char *data, size_t len, size_t start, const char *delim, size_t delim_len,
                                   size_t *positions, size_t &count, size_t max_count) {
	std::string_view key(data, len);
	std::string_view delimiter(delim, delim_len);
	size_t pos = start;
	count = 0;
	while (pos + delim_len <= len && count < max_count) {
		pos = key.find(delimiter, pos);
		if (pos == std::string_view::npos) {
			break;
		}
		positions[count++] = pos;
		pos += delim_len;
	}
}

#if defined(LEVEL_PIVOT_X86_64)

// SSE2 implementation
#if defined(_MSC_VER)
inline void find_delimiters_sse2(
#else
__attribute__((target("sse2"))) inline void find_delimiters_sse2(
#endif
    const char *data, size_t len, size_t start, const char *delim, size_t delim_len, size_t *positions, size_t &count,
    size_t max_count) {
	__m128i vd0 = _mm_set1_epi8(delim[0]);
	size_t i = start;
	count = 0;
	size_t min_next_pos = start;

	while (i + 16 <= len && count < max_count) {
		__m128i chunk = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data + i));
		__m128i eq0 = _mm_cmpeq_epi8(chunk, vd0);
		uint32_t mask0 = static_cast<uint32_t>(_mm_movemask_epi8(eq0));

		while (mask0 && count < max_count) {
#if defined(_MSC_VER)
			unsigned long bit_pos;
			_BitScanForward(&bit_pos, mask0);
#else
			uint32_t bit_pos = static_cast<uint32_t>(__builtin_ctz(mask0));
#endif
			size_t pos = i + bit_pos;

			if (pos >= min_next_pos && pos + delim_len <= len && memcmp(data + pos, delim, delim_len) == 0) {
				positions[count++] = pos;
				min_next_pos = pos + delim_len;
			}
			mask0 &= mask0 - 1;
		}
		i += 16;
	}

	// Handle tail
	while (i + delim_len <= len && count < max_count) {
		if (i >= min_next_pos && data[i] == delim[0] && memcmp(data + i, delim, delim_len) == 0) {
			positions[count++] = i;
			min_next_pos = i + delim_len;
			i += delim_len;
		} else {
			++i;
		}
	}
}

// AVX2 implementation
#if defined(_MSC_VER)
inline void find_delimiters_avx2(
#else
__attribute__((target("avx2"))) inline void find_delimiters_avx2(
#endif
    const char *data, size_t len, size_t start, const char *delim, size_t delim_len, size_t *positions, size_t &count,
    size_t max_count) {
	__m256i vd0 = _mm256_set1_epi8(delim[0]);
	size_t i = start;
	count = 0;
	size_t min_next_pos = start;

	while (i + 32 <= len && count < max_count) {
		__m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data + i));
		__m256i eq0 = _mm256_cmpeq_epi8(chunk, vd0);
		uint32_t mask0 = static_cast<uint32_t>(_mm256_movemask_epi8(eq0));

		while (mask0 && count < max_count) {
#if defined(_MSC_VER)
			unsigned long bit_pos;
			_BitScanForward(&bit_pos, mask0);
#else
			uint32_t bit_pos = static_cast<uint32_t>(__builtin_ctz(mask0));
#endif
			size_t pos = i + bit_pos;

			if (pos >= min_next_pos && pos + delim_len <= len && memcmp(data + pos, delim, delim_len) == 0) {
				positions[count++] = pos;
				min_next_pos = pos + delim_len;
			}
			mask0 &= mask0 - 1;
		}
		i += 32;
	}

	// Handle tail with scalar
	while (i + delim_len <= len && count < max_count) {
		if (i >= min_next_pos && data[i] == delim[0] && memcmp(data + i, delim, delim_len) == 0) {
			positions[count++] = i;
			min_next_pos = i + delim_len;
			i += delim_len;
		} else {
			++i;
		}
	}
}

#endif // LEVEL_PIVOT_X86_64

#if defined(LEVEL_PIVOT_AARCH64)

// NEON has no movemask equivalent — pack 16 byte-lane comparisons into a 16-bit mask
static inline uint16_t neon_movemask_u8(uint8x16_t v) {
	static const uint8_t bit_mask_vals[16] = {1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128};
	uint8x16_t bit_mask = vld1q_u8(bit_mask_vals);
	uint8x16_t masked = vandq_u8(v, bit_mask);

	uint8x8_t lo = vget_low_u8(masked);
	uint8x8_t hi = vget_high_u8(masked);
	lo = vpadd_u8(lo, lo); // 8→4
	lo = vpadd_u8(lo, lo); // 4→2
	lo = vpadd_u8(lo, lo); // 2→1
	hi = vpadd_u8(hi, hi);
	hi = vpadd_u8(hi, hi);
	hi = vpadd_u8(hi, hi);

	return static_cast<uint16_t>(vget_lane_u8(lo, 0)) | (static_cast<uint16_t>(vget_lane_u8(hi, 0)) << 8);
}

// NEON implementation (16 bytes per iteration, same throughput as SSE2)
inline void find_delimiters_neon(const char *data, size_t len, size_t start, const char *delim, size_t delim_len,
                                 size_t *positions, size_t &count, size_t max_count) {
	uint8x16_t vd0 = vdupq_n_u8(static_cast<uint8_t>(delim[0]));
	size_t i = start;
	count = 0;
	size_t min_next_pos = start;

	while (i + 16 <= len && count < max_count) {
		uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t *>(data + i));
		uint8x16_t eq0 = vceqq_u8(chunk, vd0);
		uint32_t mask0 = neon_movemask_u8(eq0);

		while (mask0 && count < max_count) {
			uint32_t bit_pos = static_cast<uint32_t>(__builtin_ctz(mask0));
			size_t pos = i + bit_pos;

			if (pos >= min_next_pos && pos + delim_len <= len && memcmp(data + pos, delim, delim_len) == 0) {
				positions[count++] = pos;
				min_next_pos = pos + delim_len;
			}
			mask0 &= mask0 - 1;
		}
		i += 16;
	}

	// Handle tail
	while (i + delim_len <= len && count < max_count) {
		if (i >= min_next_pos && data[i] == delim[0] && memcmp(data + i, delim, delim_len) == 0) {
			positions[count++] = i;
			min_next_pos = i + delim_len;
			i += delim_len;
		} else {
			++i;
		}
	}
}

#endif // LEVEL_PIVOT_AARCH64

// Function pointer type for delimiter finding
using FindDelimitersFn = void (*)(const char *data, size_t len, size_t start, const char *delim, size_t delim_len,
                                  size_t *positions, size_t &count, size_t max_count);

// Runtime dispatcher - selects best implementation once
inline FindDelimitersFn select_find_delimiters() {
#if defined(LEVEL_PIVOT_X86_64)
	const auto &cpu = CpuFeatures::get();
	if (cpu.has_avx2) {
		return find_delimiters_avx2;
	}
	if (cpu.has_sse2) {
		return find_delimiters_sse2;
	}
#endif
#if defined(LEVEL_PIVOT_AARCH64)
	const auto &cpu = CpuFeatures::get();
	if (cpu.has_neon) {
		return find_delimiters_neon;
	}
#endif
	return find_delimiters_scalar;
}

// Global function pointer - initialized once on first use
inline FindDelimitersFn get_find_delimiters() {
	static const FindDelimitersFn fn = select_find_delimiters();
	return fn;
}

} // namespace detail

/**
 * SIMD-optimized key parser for patterns with a single repeated delimiter
 *
 * This is a specialized fast-path for common patterns like:
 *   prefix##capture1##capture2##...##attr
 *
 * Uses runtime CPU detection to select SSE2/AVX2/scalar implementation.
 * Detection happens once at startup; subsequent calls have zero overhead.
 */
class SimdKeyParser {
public:
	/**
	 * Create a parser for a pattern with uniform delimiter
	 *
	 * @param prefix Literal prefix before first capture (e.g., "users")
	 * @param delimiter Delimiter between segments (e.g., "##")
	 * @param num_captures Number of capture segments (not including attr)
	 */
	SimdKeyParser(std::string_view prefix, std::string_view delimiter, size_t num_captures)
	    : prefix_(prefix), delimiter_(delimiter), num_captures_(num_captures),
	      has_prefix_delimiter_(!prefix.empty()), num_delimiters_(num_captures + (prefix.empty() ? 0 : 1)),
	      find_delimiters_(detail::get_find_delimiters()) {
	}

	/**
	 * Ultra-fast parse that returns views without Result struct allocation
	 * Returns false if no match, fills captures array directly
	 */
	bool parse_fast(std::string_view key,
	                std::string_view *captures, // Pre-allocated array
	                std::string_view &attr) const {
		// Quick prefix check
		if (key.size() < prefix_.size() + delimiter_.size() * num_delimiters_) {
			return false;
		}

		if (!prefix_.empty()) {
			if (key.substr(0, prefix_.size()) != prefix_) {
				return false;
			}
		}

		size_t search_start = prefix_.size();
		size_t delim_stack[MAX_KEY_CAPTURES + 1];
		size_t delim_count = 0;

		find_delimiters_(key.data(), key.size(), search_start, delimiter_.data(), delimiter_.size(), delim_stack,
		                 delim_count, num_delimiters_ + 1);

		if (delim_count != num_delimiters_) {
			return false;
		}

		size_t pos = prefix_.size();
		size_t delim_idx = 0;

		if (has_prefix_delimiter_) {
			// Pattern has a literal prefix: expect delimiter right after prefix
			if (delim_stack[0] != pos) {
				return false;
			}
			pos += delimiter_.size();
			delim_idx = 1;
		}

		for (size_t i = 0; i < num_captures_; ++i) {
			size_t end = delim_stack[delim_idx + i];
			if (end <= pos) {
				return false;
			}
			captures[i] = key.substr(pos, end - pos);
			pos = end + delimiter_.size();
		}

		if (pos >= key.size()) {
			return false;
		}
		attr = key.substr(pos);

		return true;
	}

	/**
	 * Get the name of the SIMD implementation being used
	 */
	static const char *implementation_name() {
#if defined(LEVEL_PIVOT_X86_64)
		const auto &cpu = detail::CpuFeatures::get();
		if (cpu.has_avx2) {
			return "AVX2";
		}
		if (cpu.has_sse2) {
			return "SSE2";
		}
#endif
#if defined(LEVEL_PIVOT_AARCH64)
		const auto &cpu = detail::CpuFeatures::get();
		if (cpu.has_neon) {
			return "NEON";
		}
#endif
		return "scalar";
	}

private:
	std::string_view prefix_;
	std::string_view delimiter_;
	size_t num_captures_;
	bool has_prefix_delimiter_;
	size_t num_delimiters_;
	detail::FindDelimitersFn find_delimiters_;
};

} // namespace level_pivot
