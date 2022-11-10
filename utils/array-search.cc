/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include "array-search.hh"
#ifdef __x86_64__
#include <x86intrin.h>
#define arch_target(name) [[gnu::target(name)]]
#else
#define arch_target(name)
#endif

namespace utils {

int array_search_gt_impl_default(int64_t val, const int64_t* array, const int capacity, const int size) {
    int i;

    for (i = 0; i < size; i++) {
        if (val < array[i])
            break;
    }

    return i;
}

static inline unsigned array_search_eq_impl(uint8_t val, const uint8_t* arr, unsigned len) {
    unsigned i;

    for (i = 0; i < len; i++) {
        if (arr[i] == val) {
            break;
        }
    }

    return i;
}

arch_target("default") unsigned array_search_16_eq_impl(uint8_t val, const uint8_t* arr) {
    return array_search_eq_impl(val, arr, 16);
}

arch_target("default") unsigned array_search_32_eq_impl(uint8_t val, const uint8_t* arr) {
    return array_search_eq_impl(val, arr, 32);
}

arch_target("default") unsigned array_search_x32_eq_impl(uint8_t val, const uint8_t* arr, int nr) {
    return array_search_eq_impl(val, arr, 32 * nr);
}

#ifdef __x86_64__

/*
 * The AVX2 version doesn't take @size argument into account and expects
 * all the elements above it to be less than any possible value.
 *
 * To make it work without this requirement we'd need to:
 *  - limit the loop iterations to size instead of capacity
 *  - explicitly set to 1 all the mask's bits for elements >= size
 * both do make things up to 50% slower.
 */

arch_target("avx2")
int array_search_gt_impl_avx2(int64_t val, const int64_t* array, const int capacity, const int size) {
    int cnt = 0;

    // 0. Load key into 256-bit ymm
    __m256i k = _mm256_set1_epi64x(val);
    for (int i = 0; i < capacity; i += 4) {
        // 4. Count the number of 1-s, each gt match gives 8 bits
        cnt += _mm_popcnt_u32(
                    // 3. Pack result into 4 bytes -- 1 byte from each comparison
                    _mm256_movemask_epi8(
                        // 2. Compare array[i] > key, 4 elements in one go
                        _mm256_cmpgt_epi64(
                            // 1. Load next 4 elements into ymm
                            _mm256_lddqu_si256((__m256i*)&array[i]), k
                        )
                    )
                ) / 8;
    }

    /*
     * 5. We need the index of the first gt value. Unused elements are < k
     *    for sure, so count from the tail of the used part.
     *
     *   <grumble>
     *    We might have done it the other way -- store the maximum in unused,
     *    check for key >= array[i] in the above loop and just return the cnt,
     *    but ...  AVX2 instructions set doesn't have the PCMPGE
     *
     *    SSE* set (predecessor) has cmpge, but eats 2 keys in one go
     *    AVX-512 (successor) has it back, and even eats 8 keys, but is
     *    not widely available
     *   </grumble>
     */
    return size - cnt;
}

arch_target("avx512f")
int array_search_16_gt_impl_avx512f(int64_t val, const int64_t* array, const int capacity, const int size) {
    auto a = _mm512_set1_epi64(val);
    auto b1 = _mm512_loadu_si512((__m512i*)array);
    auto b2 = _mm512_loadu_si512((__m512i*)array + 1);
    auto c1 = _mm512_cmp_epi64_mask(a, b1, _MM_CMPINT_GT);
    auto c2 = _mm512_cmp_epi64_mask(a, b2, _MM_CMPINT_GT);
    auto d = _mm512_kunpackb(c1, c2);
    auto e = _cvtmask16_u32(d);
    auto f = __builtin_popcount(e);
    return size - f;
}

/*
 * SSE4 version of searching in array for an exact match.
 */
arch_target("sse") unsigned array_search_16_eq_impl(uint8_t val, const uint8_t* arr) {
	auto a = _mm_set1_epi8(val);
	auto b = _mm_lddqu_si128((__m128i*)arr);
	auto c = _mm_cmpeq_epi8(a, b);
	unsigned int m = _mm_movemask_epi8(c);
	return __builtin_ctz(m | 0x10000);
}

/*
 * AVX2 version of searching in array for an exact match.
 */
arch_target("avx2") unsigned array_search_32_eq_impl(uint8_t val, const uint8_t* arr) {
    auto a = _mm256_set1_epi8(val);
    auto b = _mm256_lddqu_si256((__m256i*)arr);
    auto c = _mm256_cmpeq_epi8(a, b);
    unsigned long long m = _mm256_movemask_epi8(c);
    return __builtin_ctzll(m | 0x100000000ull);
}

arch_target("avx2") unsigned array_search_x32_eq_impl(uint8_t val, const uint8_t* arr, int nr) {
    unsigned len = 32 * nr;
    auto a = _mm256_set1_epi8(val);
    for (unsigned off = 0; off < len; off += 32) {
        auto b = _mm256_lddqu_si256((__m256i*)arr);
        auto c = _mm256_cmpeq_epi8(a, b);
        unsigned m = _mm256_movemask_epi8(c);
        if (m != 0) {
            return __builtin_ctz(m) + off;
        }
    }
    return len;
}

#endif

extern "C" decltype(&array_search_gt) array_search_gt_impl_resolver() {
#ifdef __x86_64__
    __builtin_cpu_init();
    if (__builtin_cpu_supports("avx2")) {
        return array_search_gt_impl_avx2;
    }
#endif
    return array_search_gt_impl_default;
}

[[gnu::ifunc("array_search_gt_impl_resolver")]]
decltype(array_search_gt) array_search_gt;

extern "C" decltype(&array_search_16_gt) array_search_16_gt_impl_resolver() {
#ifdef __x86_64__
    __builtin_cpu_init();
    if (__builtin_cpu_supports("avx512f")
            // Disable AVX-512 for CPUs affected by licence-based downclocking
            && !__builtin_cpu_is("skylake-avx512")
            && !__builtin_cpu_is("cascadelake")
            && !__builtin_cpu_is("cooperlake")
            && !__builtin_cpu_is("cannonlake")
    ) {
        return array_search_16_gt_impl_avx512f;
    }
    if (__builtin_cpu_supports("avx2")) {
        return array_search_gt_impl_avx2;
    }
#endif
    return array_search_gt_impl_default;
}

[[gnu::ifunc("array_search_16_gt_impl_resolver")]]
decltype(array_search_16_gt) array_search_16_gt;

unsigned array_search_16_eq(uint8_t val, const uint8_t* arr) {
    return array_search_16_eq_impl(val, arr);
}

unsigned array_search_32_eq(uint8_t val, const uint8_t* array) {
    return array_search_32_eq_impl(val, array);
}

unsigned array_search_x32_eq(uint8_t val, const uint8_t* array, int nr) {
    return array_search_x32_eq_impl(val, array, nr);
}

}
