/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "test/lib/scylla_test_case.hh"

#include <seastar/util/defer.hh>
#include <seastar/core/memory.hh>
#include "utils/base64.hh"
#include "utils/rjson.hh"
#include "alternator/serialization.hh"
#include "alternator/error.hh"

#include "alternator/http_compression.hh"
#include "test/lib/log.hh"
#include "test/lib/random_utils.hh"
#include "utils/small_vector.hh"
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>
#include <zlib.h>
#include <bit>
#include <chrono>
#include <random>

#include "cdc/generation.hh"
#include "alternator/executor.hh"
#include "dht/token-sharding.hh"
#include "alternator/expressions.hh"
#include "alternator/streams.hh"
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/sleep.hh>

namespace alternator {
    const cdc::stream_id& find_parent_shard_in_previous_generation(db_clock::time_point prev_timestamp, const utils::chunked_vector<cdc::stream_id>& prev_streams, const cdc::stream_id& child);
}

BOOST_AUTO_TEST_CASE(test_extract_table_name_from_arn_simple_new_format) {
    std::string_view arn = "arn:aws:dynamodb:us-east-1:797456418907:table/ks_space@dynamodb_streams_verification_table_rc/stream/2025-12-18T17:38:48.952";

    auto parts = alternator::parse_arn(arn, "StreamArn", "stream", "/stream/");
    BOOST_REQUIRE_EQUAL(parts.table_name, "dynamodb_streams_verification_table_rc");
    BOOST_REQUIRE_EQUAL(parts.keyspace_name, "ks_space");
    BOOST_REQUIRE_EQUAL(parts.postfix, "/stream/2025-12-18T17:38:48.952");
}

BOOST_AUTO_TEST_CASE(test_extract_table_name_from_arn_simple_old_format) {
    std::string_view arn = "arn:scylla:service:region:account-id:table/resource";

    auto parts = alternator::parse_arn(arn, "ResourceArn", "table", "");
    BOOST_REQUIRE_EQUAL(parts.table_name, "resource");
    BOOST_REQUIRE_EQUAL(parts.keyspace_name, "region");
    BOOST_REQUIRE_EQUAL(parts.postfix, "");
}

BOOST_AUTO_TEST_CASE(test_extract_table_name_from_arn_no_table) {
    std::string_view arn = "arn:aws:dynamodb:us-east-1:797456418907:foo/dynamodb_streams_verification_table_rc/stream/2025-12-18T17:38:48.952";

    BOOST_REQUIRE_THROW(alternator::parse_arn(arn, "", "", ""), alternator::api_error);
}

BOOST_AUTO_TEST_CASE(test_extract_table_name_from_arn_no_keyspace) {
    std::string_view arn = "arn:aws:dynamodb:us-east-1:797456418907:table/dynamodb_streams_verification_table_rc";

    BOOST_REQUIRE_THROW(alternator::parse_arn(arn, "", "", ""), alternator::api_error);
}

BOOST_AUTO_TEST_CASE(test_extract_table_name_from_arn_wrong_postfix) {
    std::string_view arn = "arn:aws:dynamodb:us-east-1:797456418907:table/dynamodb_streams_verification_table_rc/stream/2025-12-18T17:38:48.952";

    BOOST_REQUIRE_THROW(alternator::parse_arn(arn, "", "", "/cakes"), alternator::api_error);
}

BOOST_AUTO_TEST_CASE(test_extract_table_name_from_arn_not_enough_colons_1) {
    std::string_view arn = "arn:aws:dynamodb:us-east-1:797456418907";

    BOOST_REQUIRE_THROW(alternator::parse_arn(arn, "", "", ""), alternator::api_error);
}

BOOST_AUTO_TEST_CASE(test_extract_table_name_from_arn_not_enough_colons_2) {
    std::string_view arn = "arn";

    BOOST_REQUIRE_THROW(alternator::parse_arn(arn, "", "", ""), alternator::api_error);
}

BOOST_AUTO_TEST_CASE(test_extract_table_name_from_arn_empty) {
    std::string_view arn = "";

    BOOST_REQUIRE_THROW(alternator::parse_arn(arn, "", "", ""), alternator::api_error);
}

static std::map<std::string, std::string> strings {
    {"", ""},
    {"a", "YQ=="},
    {"ab", "YWI="},
    {"abc", "YWJj"},
    {"abcd", "YWJjZA=="},
    {"abcde", "YWJjZGU="},
    {"abcdef", "YWJjZGVm"},
    {"abcdefg", "YWJjZGVmZw=="},
    {"abcdefgh", "YWJjZGVmZ2g="},
};

BOOST_AUTO_TEST_CASE(test_base64_encode_decode) {
    for (auto& [str, encoded] : strings) {
        BOOST_REQUIRE_EQUAL(base64_encode(to_bytes_view(str)), encoded);
        auto decoded = base64_decode(encoded);
        BOOST_REQUIRE_EQUAL(to_bytes_view(str), bytes_view(decoded));
    }
}

BOOST_AUTO_TEST_CASE(test_base64_decoded_len) {
    for (auto& [str, encoded] : strings) {
        BOOST_REQUIRE_EQUAL(str.size(), base64_decoded_len(encoded));
    }
}

BOOST_AUTO_TEST_CASE(test_base64_begins_with) {
    for (auto& [str, encoded] : strings) {
        for (size_t i = 0; i < str.size(); ++i) {
            std::string prefix(str.c_str(), i);
            std::string encoded_prefix = base64_encode(to_bytes_view(prefix));
            BOOST_REQUIRE(base64_begins_with(encoded, encoded_prefix));
        }
    }
    std::string str1 = "ABCDEFGHIJKL123456";
    std::string str2 = "ABCDEFGHIJKL1234567";
    std::string str3 = "ABCDEFGHIJKL12345678";
    std::string encoded_str1 = base64_encode(to_bytes_view(str1));
    std::string encoded_str2 = base64_encode(to_bytes_view(str2));
    std::string encoded_str3 = base64_encode(to_bytes_view(str3));
    std::vector<std::string> non_prefixes = {
        "B", "AC", "ABD", "ACD", "ABCE", "ABCEG", "ABCDEFGHIJKLM", "ABCDEFGHIJKL123456789"
    };
    for (auto& non_prefix : non_prefixes) {
        std::string encoded_non_prefix = base64_encode(to_bytes_view(non_prefix));
        BOOST_REQUIRE(!base64_begins_with(encoded_str1, encoded_non_prefix));
        BOOST_REQUIRE(!base64_begins_with(encoded_str2, encoded_non_prefix));
        BOOST_REQUIRE(!base64_begins_with(encoded_str3, encoded_non_prefix));
    }
}

BOOST_AUTO_TEST_CASE(test_allocator_fail_gracefully) {
    // Allocation size is set to a ridiculously high value to ensure
    // that it will immediately fail - trying to lazily allocate just
    // a little more than total memory may still succeed.
    static size_t too_large_alloc_size = memory::stats().total_memory() * 1024 * 1024;
    rjson::allocator allocator;
    // Impossible allocation should throw
    BOOST_REQUIRE_THROW(allocator.Malloc(too_large_alloc_size), rjson::error);
    // So should impossible reallocation
    void* memory = allocator.Malloc(1);
    auto release = defer([memory] noexcept { rjson::allocator::Free(memory); });
    BOOST_REQUIRE_THROW(allocator.Realloc(memory, 1, too_large_alloc_size), rjson::error);
    // Internal rapidjson stack should also throw
    // and also be destroyed gracefully later
    rapidjson::internal::Stack stack(&allocator, 0);
    BOOST_REQUIRE_THROW(stack.Push<char>(too_large_alloc_size), rjson::error);
}

// Test the alternator::internal::magnitude_and_precision() function which we
// use to used to check if a number exceeds DynamoDB's limits on magnitude and
// precision (for issue #6794). This just tests the internal implementation -
// we also have end-to-end tests trying to insert various numbers with bad
// magnitude and precision to the database in test/alternator/test_number.py.
BOOST_AUTO_TEST_CASE(test_magnitude_and_precision) {
    struct expected {
        const char* number;
        int magnitude;
        int precision;
    };
    std::vector<expected> tests = {
        // number     magnitude, precision
        {"0",         0, 0},
        {"0e10",      0, 0},
        {"0e-10",     0, 0},
        {"0e+10",     0, 0},
        {"0.0",       0, 0},
        {"0.00e10",   0, 0},
        {"1",         0, 1},
        {"12.",       1, 2},
        {"1.1",       0, 2},
        {"12.3",      1, 3},
        {"12.300",    1, 3},
        {"0.3",       -1, 1},
        {".3",        -1, 1},
        {"3e-1",      -1, 1},
        {"0.00012",   -4, 2},
        {"1.2e-4",    -4, 2},
        {"1.2E-4",    -4, 2},
        {"12.345e50", 51, 5},
        {"12.345e-50",-49, 5},
        {"123000000", 8, 3},
        {"123000000.000e+5", 13, 3},
        {"10.01",     1, 4},
        {"1.001e1",   1, 4},
        {"1e5",       5, 1},
        {"1e+5",      5, 1},
        {"1e-5",      -5, 1},
        {"123e-7",    -5, 3},
        // These are important edge cases: DynamoDB considers 1e126 to be
        // overflowing but 9.9999e125 is considered to have magnitude 125
        // and ok. Conversely, 1e-131 is underflowing and 0.9e-130 is too.
        {"9.99999e125", 125, 6},
        {"0.99999e-130", -131, 5},
        {"0.9e-130", -131, 1},
        // Although 1e1000 is not allowed, 0e0000 is allowed - it's just 0.
        {"0e1000",    0, 0},
    };
    // prefixes that should do nothing to a number
    std::vector<std::string> prefixes = {
        "",
        "0",
        "+",
        "-",
        "+0000",
        "-0000"
    };
    for (expected test : tests) {
        for (std::string prefix : prefixes) {
            std::string number = prefix + test.number;
            auto res = alternator::internal::get_magnitude_and_precision(number);
            BOOST_CHECK_MESSAGE(res.magnitude == test.magnitude,
                seastar::format("{}: expected magnitude {}, got {}", number, test.magnitude, res.magnitude));
            BOOST_CHECK_MESSAGE(res.precision == test.precision,
                seastar::format("{}: expected precision {}, got {}", number, test.precision, res.precision));
        }
    }
    // Huge exponents like 1e1000000 are not guaranteed to return that
    // specific number as magnitude, but is guaranteed to return some
    // other high magnitude that the caller can complain is excessive.
    auto res = alternator::internal::get_magnitude_and_precision("1e1000000");
    BOOST_CHECK(res.magnitude > 1000);
    res = alternator::internal::get_magnitude_and_precision("1e-1000000");
    BOOST_CHECK(res.magnitude < -1000);
    // Even if an exponent so huge that it doesn't even fit in a 32-bit
    // integer, we shouldn't fail to recognize its excessive magnitude:
    res = alternator::internal::get_magnitude_and_precision("1e1000000000000");
    BOOST_CHECK(res.magnitude > 1000);
    res = alternator::internal::get_magnitude_and_precision("1e-1000000000000");
    BOOST_CHECK(res.magnitude < -1000);
}

// parsed expression cache tests:

// ANTLR3 leaks memory when it tries to recover from missing token.
// - it creates a "fake" token, if it allows to continue parsing.
// Leak was reported by ASAN, when running this test in debug mode - 
// the test passed but the leak is discovered when the test file exits.
// Reproduces #25878
BOOST_AUTO_TEST_CASE(missing_tokens_memory_leak) {
    BOOST_REQUIRE_THROW(alternator::parse_update_expression("SET a :v"), alternator::expressions_syntax_error); // missing '='
    BOOST_REQUIRE_THROW(alternator::parse_update_expression("DELETE a v"), alternator::expressions_syntax_error); // missing ':'
    BOOST_REQUIRE_THROW(alternator::parse_update_expression("ADD a v"), alternator::expressions_syntax_error); // missing ':'
    
    BOOST_REQUIRE_THROW(alternator::parse_condition_expression("size(a < 5", "Test"), alternator::expressions_syntax_error); // missing ')'
    BOOST_REQUIRE_THROW(alternator::parse_condition_expression("a IN :x)", "Test"), alternator::expressions_syntax_error); // missing '('
    BOOST_REQUIRE_THROW(alternator::parse_condition_expression("a IN (:x", "Test"), alternator::expressions_syntax_error); // missing ')'
    BOOST_REQUIRE_THROW(alternator::parse_condition_expression("a BETWEEN :x AN :y", "Test"), alternator::expressions_syntax_error); // missing 'AND'
    BOOST_REQUIRE_THROW(alternator::parse_condition_expression("a BETWEEN :x :y", "Test"), alternator::expressions_syntax_error); // missing 'AND'

    BOOST_REQUIRE_THROW(alternator::parse_projection_expression("a[0.b"), alternator::expressions_syntax_error); // missing ']'
}

// Tests of inputs that cause exceptions inside the expression parser.
// ANTR3 itself doesn't use exceptions, but we do in additional checks.
// Apart from correct response, which may be tested in Python tests,
// main concern here is if this can cause memory leaks
// similar to issue in the above test.
BOOST_AUTO_TEST_CASE(exception_at_expression_parsing) {
    // std::stoi throws std::out_of_range if the number is too big
    BOOST_REQUIRE_THROW(alternator::parse_projection_expression("a[99999999999999999]") , alternator::expressions_syntax_error);

    // Path depth limit exceeded should throw expressions_syntax_error
    // alternator::parsed::path::depth_limit is private, so try with some arbitrary long path:
    std::string long_path = "a";
    for (int i = 0; i < 100; ++i) {
        long_path += ".a";
    }
    BOOST_REQUIRE_THROW(alternator::parse_projection_expression(long_path), alternator::expressions_syntax_error);

    // Appending duplicate update actions throws expressions_syntax_error
    BOOST_REQUIRE_THROW(alternator::parse_update_expression("SET a = :v SET b = :w"), alternator::expressions_syntax_error);

    // Single non-function condition throws expressions_syntax_error
    BOOST_REQUIRE_THROW(alternator::parse_condition_expression("a OR b", "TEST"), alternator::expressions_syntax_error);
}

using exp_type = alternator::stats::expression_types;
static int exp_type_i(exp_type type) {
    if (static_cast<int>(type) >= exp_type::NUM_EXPRESSION_TYPES)
        BOOST_FAIL("Invalid expression type");
    return static_cast<int>(type);
}
static std::string_view str(exp_type type) {
    constexpr static std::string_view exp_type_s[exp_type::NUM_EXPRESSION_TYPES] = { "projection", "update", "condition" };
    return exp_type_s[exp_type_i(type)];
};
static uint64_t& hits_counter(alternator::stats& stats, exp_type type) {
    return stats.expression_cache.requests[exp_type_i(type)].hits;
}
static uint64_t& misses_counter(alternator::stats& stats, exp_type type) {
    return stats.expression_cache.requests[exp_type_i(type)].misses;
}
enum class expecting_exception { yes, no };
static expecting_exception hit(alternator::stats& stats, exp_type type) {
    hits_counter(stats, type)++;
    return expecting_exception::no;
}
static expecting_exception miss(alternator::stats& stats, exp_type type) {
    misses_counter(stats, type)++;
    return expecting_exception::no;
}
static expecting_exception eviction_miss(alternator::stats& stats, exp_type type) {
    stats.expression_cache.evictions++;
    return miss(stats, type);
}
static expecting_exception invalid(alternator::stats& stats, exp_type type) {
    return expecting_exception::yes;
}
struct test_cache {
    alternator::stats stats;
    alternator::stats expected_stats;
    utils::updateable_value_source<uint32_t> max_cache_entries;
    std::unique_ptr<alternator::parsed::expression_cache> cache;
    test_cache(int size) : max_cache_entries(size), cache(std::make_unique<alternator::parsed::expression_cache>(alternator::parsed::expression_cache::config{
        .max_cache_entries = utils::updateable_value<uint32_t>(max_cache_entries)
    }, stats)) {}

    std::string validate_stats(const std::string& msg) {
        for (int t = 0; t < exp_type::NUM_EXPRESSION_TYPES; t++) {
            exp_type type = static_cast<exp_type>(t);
            if(hits_counter(stats, type) != hits_counter(expected_stats, type)) {
                return format("{}: expected {} {} hits, got {}", msg, hits_counter(expected_stats, type), str(type), hits_counter(stats, type));
            }
            if(misses_counter(stats, type) != misses_counter(expected_stats, type)) {
                return format("{}: expected {} {} misses, got {}", msg, misses_counter(expected_stats, type), str(type), misses_counter(stats, type));
            }
        }
        if(stats.expression_cache.evictions != expected_stats.expression_cache.evictions) {
            return format("{}: expected {} evictions, got {}", msg, expected_stats.expression_cache.evictions, stats.expression_cache.evictions);
        }
        return std::string();
    }
    void check_stats(const std::string& msg) {
        std::string v = validate_stats(msg);
        BOOST_REQUIRE_MESSAGE(v.empty(), v);
    }
    seastar::future<> wait_check_stats(const std::string& msg) {
        for (int attempt = 0; attempt < 100; attempt++) {
            std::string v = validate_stats(msg);
            if (v.empty()) {
                co_return;
            }
            co_await seastar::sleep(std::chrono::milliseconds(10));
        }
        check_stats(msg); // Final check after all attempts
    }
    void try_parse(const std::string& expr, exp_type type, expecting_exception (*expected_cache_behavior)(alternator::stats&, exp_type)) {
        try {
            switch (type) {
            case exp_type::PROJECTION_EXPRESSION:
                (void)(cache->parse_projection_expression(expr));
                break;
            case exp_type::UPDATE_EXPRESSION:
                (void)(cache->parse_update_expression(expr));
                break;
            case exp_type::CONDITION_EXPRESSION:
                (void)(cache->parse_condition_expression(expr, "Test"));
                break;
            default:
                BOOST_FAIL("Invalid expression type");
            }
            if (expected_cache_behavior(expected_stats, type) == expecting_exception::yes) {
                BOOST_FAIL(format("Expected exception for {} expression: {}, but none was thrown.", str(type), expr));
            }
        } catch (const alternator::expressions_syntax_error& ex) {
            if (expected_cache_behavior(expected_stats, type) == expecting_exception::no) {
                BOOST_FAIL(format("Unexpected syntax exception for {} expression: {}, {}", str(type), expr, ex.what()));
            }
        } catch (const std::exception& ex) {
            BOOST_FAIL(format("Unexpected exception for {} expression: {}, {}", str(type), expr, ex.what()));
        }
        check_stats(format("after parsing {} expression: {}", str(type), expr));
    }
};

// Basic cache functionality test: hits, misses, evictions.
SEASTAR_TEST_CASE(test_parsed_expression_cache) {
    test_cache cache(3);

    // New entries
    cache.try_parse("a", exp_type::PROJECTION_EXPRESSION, miss);
    cache.try_parse("a", exp_type::PROJECTION_EXPRESSION, hit);
    cache.try_parse("SET a=:v", exp_type::UPDATE_EXPRESSION, miss);
    cache.try_parse("SET a=:v", exp_type::UPDATE_EXPRESSION, hit);
    cache.try_parse("a=:v", exp_type::CONDITION_EXPRESSION, miss);
    cache.try_parse("a=:v", exp_type::CONDITION_EXPRESSION, hit);

    // Cache full - evicting old entrires
    cache.try_parse("b", exp_type::PROJECTION_EXPRESSION, eviction_miss);
    cache.try_parse("b", exp_type::PROJECTION_EXPRESSION, hit);
    cache.try_parse("SET b=:v", exp_type::UPDATE_EXPRESSION, eviction_miss);
    cache.try_parse("SET b=:v", exp_type::UPDATE_EXPRESSION, hit);
    cache.try_parse("b=:v", exp_type::CONDITION_EXPRESSION, eviction_miss);
    cache.try_parse("b=:v", exp_type::CONDITION_EXPRESSION, hit);

    // Keys existing in cache, but invalid (for a given type) - raise exception
    cache.try_parse("b", exp_type::UPDATE_EXPRESSION, invalid);
    cache.try_parse("b", exp_type::CONDITION_EXPRESSION, invalid);
    cache.try_parse("SET b=:v", exp_type::PROJECTION_EXPRESSION, invalid);
    cache.try_parse("SET b=:v", exp_type::CONDITION_EXPRESSION, invalid);
    cache.try_parse("b=:v", exp_type::PROJECTION_EXPRESSION, invalid);
    cache.try_parse("b=:v", exp_type::UPDATE_EXPRESSION, invalid);

    // Invalid expressions should not affect cache state
    cache.try_parse("b", exp_type::PROJECTION_EXPRESSION, hit);
    cache.try_parse("SET b=:v", exp_type::UPDATE_EXPRESSION, hit);
    cache.try_parse("b=:v", exp_type::CONDITION_EXPRESSION, hit);

    co_return;
}

// Test that same strings can't be parsed to different expression types.
SEASTAR_TEST_CASE(test_parsed_expression_cache_invalid_requests) {
    test_cache cache(2000);

    auto inv_expr = {"", " ", "SET", "set", ":v", "1"};
    for (auto expr : inv_expr) {
        cache.try_parse(expr, exp_type::PROJECTION_EXPRESSION, invalid);
        cache.try_parse(expr, exp_type::UPDATE_EXPRESSION, invalid);
        cache.try_parse(expr, exp_type::CONDITION_EXPRESSION, invalid);
    }
    auto projection = {"a", "a, b", "a.b", "a.#b", "#a[1]", "a[1].b"};
    for (auto expr : projection) {
        cache.try_parse(expr, exp_type::UPDATE_EXPRESSION, invalid);
        cache.try_parse(expr, exp_type::CONDITION_EXPRESSION, invalid);
        cache.try_parse(expr, exp_type::PROJECTION_EXPRESSION, miss);
    }
    auto condition = {"a=:v", "size(a)", "a IN (:v)", "a > :v", "a = :v AND b = :w", "a = :v OR b = :w", "NOT a = :v", "(a = :v)"};
    for (auto expr : condition) {
        cache.try_parse(expr, exp_type::PROJECTION_EXPRESSION, invalid);
        cache.try_parse(expr, exp_type::UPDATE_EXPRESSION, invalid);
        cache.try_parse(expr, exp_type::CONDITION_EXPRESSION, miss);
    }
    auto update = {"SET a=:v", "SET a=:v, b = :1", "ADD a[1] :v", "REMOVE a[1]", "DELETE a :v", "DELETE a :v, b :w REMOVE c", "SET a=:v REMOVE b ADD c :w"};
    for (auto expr : update) {
        cache.try_parse(expr, exp_type::PROJECTION_EXPRESSION, invalid);
        cache.try_parse(expr, exp_type::CONDITION_EXPRESSION, invalid);
        cache.try_parse(expr, exp_type::UPDATE_EXPRESSION, miss);
    }
    co_return;
}

// Test resizing the cache at runtime.
SEASTAR_TEST_CASE(test_parsed_expression_cache_resize) {
    test_cache cache(3);

    cache.try_parse("a", exp_type::PROJECTION_EXPRESSION, miss);
    cache.try_parse("b", exp_type::PROJECTION_EXPRESSION, miss);
    cache.try_parse("c", exp_type::PROJECTION_EXPRESSION, miss);
    cache.try_parse("d", exp_type::PROJECTION_EXPRESSION, eviction_miss);

    cache.max_cache_entries.set(4);
    cache.try_parse("e", exp_type::PROJECTION_EXPRESSION, miss);

    cache.max_cache_entries.set(2);
    cache.expected_stats.expression_cache.evictions += 2;
    cache.check_stats("after resizing cache to 2 entries");

    cache.max_cache_entries.set(0);
    cache.expected_stats.expression_cache.evictions += 2;
    cache.check_stats("after disabling cache");

    // for resizes down with more then 3000 evictions the change may be asynchronous
    size_t large_size = 30000;
    size_t first_reduce = 75*large_size/100;
    cache.max_cache_entries.set(large_size);
    for (size_t i = 0; i < large_size; i++) {
        cache.try_parse(seastar::format("expr{}", i), exp_type::PROJECTION_EXPRESSION, miss);
        co_await coroutine::maybe_yield();
    }
    cache.max_cache_entries.set(first_reduce);
    cache.expected_stats.expression_cache.evictions += (large_size - first_reduce);
    co_await cache.wait_check_stats("async, after resizing cache");
    for (size_t i = 0; i < first_reduce; i++) {
        cache.try_parse(seastar::format("expr{}", i), exp_type::PROJECTION_EXPRESSION, eviction_miss);
        co_await coroutine::maybe_yield();
    }
    cache.max_cache_entries.set(0);
    cache.expected_stats.expression_cache.evictions += first_reduce;
    co_await cache.wait_check_stats("async, after disabling cache");

    cache.max_cache_entries.set(large_size);
    for (size_t i = 0; i < large_size; i++) {
        cache.try_parse(seastar::format("expr{}", i), exp_type::PROJECTION_EXPRESSION, miss);
        co_await coroutine::maybe_yield();
    }
    cache.max_cache_entries.set(1000);
    co_await cache.cache->stop();
    cache.cache.reset();

    co_return;
}

static cdc::stream_id generate_stream_id_from_int(std::int64_t val)
{
    auto token = dht::token::from_int64(val);
    return cdc::stream_id{ token, 1 };
}

static utils::chunked_vector<cdc::stream_id> generate_streams_generation(std::initializer_list<std::int64_t> vals)
{
    utils::chunked_vector<cdc::stream_id> gen;
    for (auto val : vals) {
        gen.push_back(generate_stream_id_from_int(val));
    }
    return gen;
}

BOOST_AUTO_TEST_CASE(find_parent_shard_in_previous_generation) {

    auto gen = generate_streams_generation({ -10, 10 });

    BOOST_CHECK(alternator::find_parent_shard_in_previous_generation({}, gen, generate_stream_id_from_int(-20)) == gen[0]);
    BOOST_CHECK(alternator::find_parent_shard_in_previous_generation({}, gen, generate_stream_id_from_int(-10)) == gen[0]);
    BOOST_CHECK(alternator::find_parent_shard_in_previous_generation({}, gen, generate_stream_id_from_int(0)) == gen[1]);
    BOOST_CHECK(alternator::find_parent_shard_in_previous_generation({}, gen, generate_stream_id_from_int(10)) == gen[1]);
    BOOST_CHECK(alternator::find_parent_shard_in_previous_generation({}, gen, generate_stream_id_from_int(20)) == gen[0]);
}

BOOST_AUTO_TEST_CASE(find_parent_shard_in_previous_generation_one_value) {
    auto gen = generate_streams_generation({ -10 });

    BOOST_CHECK(alternator::find_parent_shard_in_previous_generation({}, gen, generate_stream_id_from_int(-20)) == gen[0]);
    BOOST_CHECK(alternator::find_parent_shard_in_previous_generation({}, gen, generate_stream_id_from_int(-10)) == gen[0]);
    BOOST_CHECK(alternator::find_parent_shard_in_previous_generation({}, gen, generate_stream_id_from_int(0)) == gen[0]);
}

namespace {
    auto sid(std::int64_t token) {
        return cdc::stream_id{ dht::token{ token }, 0 };
    }
    auto stream_ids(std::initializer_list<cdc::stream_id> ids) {
        utils::chunked_vector<cdc::stream_id> result;
        result.reserve(ids.size());
        for (const auto& id : ids) {
            result.push_back(id);
        }
        return result;
    }
    auto sids(std::initializer_list<std::int64_t> tokens) {
        utils::chunked_vector<cdc::stream_id> result;
        for (auto t : tokens) {
            result.push_back(sid(t));
        }
        return result;
    }
    utils::chunked_vector<cdc::stream_id> to_sids(alternator::stream_id_range range) {
        utils::chunked_vector<cdc::stream_id> result;
        range.prepare_for_iterating();
        for (auto &sid : range) {
            result.push_back(sid);
        }
        return result;
    }
    utils::chunked_vector<cdc::stream_id> vec(const utils::chunked_vector<cdc::stream_id> &vec, int start = 0, int end = 0x7fffffff, int start2 = 0, int end2 = 0) {
        auto update_start_end = [&](int &start, int &end) {
            if (start < 0) start += (int)vec.size();
            if (end < 0) end += (int)vec.size();
            if (start < 0) start = 0;
            if (start > (int)vec.size()) start = (int)vec.size();
            if (end < 0) end = 0;
            if (end > (int)vec.size()) end = (int)vec.size();
        };
        update_start_end(start, end);
        update_start_end(start2, end2);
        utils::chunked_vector<cdc::stream_id> result;
        while(start < end) {
            result.push_back(vec[start++]);
        }
        while(start2 < end2) {
            result.push_back(vec[start2++]);
        }
        return result;
    }
    utils::chunked_vector<cdc::stream_id> sorted_vec(utils::chunked_vector<cdc::stream_id> v, int start = 0, int end = 0x7fffffff, int start2 = 0, int end2 = 0) {
        std::sort(v.begin(), v.end(), [](const cdc::stream_id &a, const cdc::stream_id &b) {
            return a.token() < b.token();
        });
        auto v2 = vec(v, start, end, start2, end2);
        std::sort(v2.begin(), v2.end(), [](const cdc::stream_id &a, const cdc::stream_id &b) {
            return compare_unsigned(a.to_bytes(), b.to_bytes()) < 0;
        });
        return v2;
    }
}

namespace cdc {
    // must be in cdc namespace so ADL could work and BOOST could find it
    std::ostream & operator <<(std::ostream &os, const std::vector<stream_id> &vec) {
        os << "[";
        bool first = true;
        for (auto &sid : vec) {
            if (!first) {
                os << ", ";
            }
            first = false;
            os << sid.token();
        }
        os << "]";
        return os;
    }
}
namespace utils {
    std::ostream & operator <<(std::ostream &os, const utils::chunked_vector<cdc::stream_id> &vec) {
        os << "[";
        bool first = true;
        for (auto &sid : vec) {
            if (!first) {
                os << ", ";
            }
            first = false;
            os << sid.token();
        }
        os << "]";
        return os;
    }
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_simple) {
    auto parent_streams = sids({ -50, 50, std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ -50, 50, std::numeric_limits<std::int64_t>::max() });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_starting_pos_1) {
    auto parent_streams = sids({ -150, 200, std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ -200, -150, -100, -50, 0, 50, 100, 150, 200, 250, std::numeric_limits<std::int64_t>::max() });
    auto starting_pos = current_streams[5]; // 50, we need to pick it now, because find_children_range_from_parent_token will sort and move current_streams

    // we expect to get 100, 150, 200, -100, -50
    auto expected_result = sorted_vec(current_streams, 2, 4, 6, 9);

    // we search for children of parent at token 200, which means all children that touch range (-150, 200]
    // the range will be (-150, 200], but sorted unsigned, so (0, 200] and (-150, 0) -> 0, 50, 100, 150, 200, -100, -50
    auto range = alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], true);

    // let's start from 50 - this skips 0 and 50 as starting point is exclusive
    range.set_starting_position(starting_pos);

    // we should get 100, 150, 200, -100, -50
    auto range_sids = to_sids(range);
    BOOST_REQUIRE(range_sids == expected_result);
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_starting_pos_2) {
    auto parent_streams = sids({ -150, 200, std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ -200, -150, -100, -50, 0, 50, 100, 150, 200, 250, std::numeric_limits<std::int64_t>::max() });
    auto starting_pos = current_streams[2]; // -100, we need to pick it now, because find_children_range_from_parent_token will sort and move current_streams

    // we expect to get -50
    auto expected_result = sorted_vec(current_streams, 3, 4);

    // we search for children of parent at token 200, which means all children that touch range (-150, 200]
    // the range will be (-150, 200], but sorted unsigned, so (0, 200] and (-150, 0) -> 0, 50, 100, 150, 200, -100, -50
    auto range = alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], true);

    // let's start from -100 - this leaves only -50 and skips everything before it
    range.set_starting_position(starting_pos);

    // we should get -50
    auto range_sids = to_sids(range);
    BOOST_REQUIRE(range_sids == expected_result);
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_merge_1) {
    auto parent_streams = sids({ 0, 50, std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ 0, std::numeric_limits<std::int64_t>::max() });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_merge_2) {
    auto parent_streams = sids({ 0, 50, std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ 50, std::numeric_limits<std::int64_t>::max() });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_merge_into_one) {
    auto parent_streams = sids({ -100, -50, 25, 50, std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ std::numeric_limits<std::int64_t>::max() });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[3], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[4], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_split_1) {
    auto parent_streams = sids({ 0, 50, std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ 0, 25, 50, std::numeric_limits<std::int64_t>::max() });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 3));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 3, 4));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_split_2) {
    auto parent_streams = sids({ 0, 50, std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ -25, 0, 50, std::numeric_limits<std::int64_t>::max() });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 3, 4));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_split_3) {
    auto parent_streams = sids({ 0, 50, std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ 0, 50, 75, std::numeric_limits<std::int64_t>::max() });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 4));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_split_from_one) {
    auto parent_streams = sids({ std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ -100, -50, 50, 75, std::numeric_limits<std::int64_t>::max() });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 5));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_tablets_split_and_merge) {
    auto parent_streams = sids({ 0, 50, 100, std::numeric_limits<std::int64_t>::max() });
    auto current_streams = sids({ 25, 75, std::numeric_limits<std::int64_t>::max() });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 3));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[3], true));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));
}







BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_simple) {
    auto parent_streams = sids({ -50, 50 });
    auto current_streams = sids({ -50, 50 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_starting_pos_1) {
    auto parent_streams = sids({ -150, 200 });
    auto current_streams = sids({ -200, -150, -100, -50, 0, 50, 100, 150, 200, 250 });
    auto starting_pos = current_streams[5]; // 50, we need to pick it now, because find_children_range_from_parent_token will sort and move current_streams

    // we expect to get 100, 150, 200, -100, -50
    auto expected_result = sorted_vec(current_streams, 2, 4, 6, 9);

    // we search for children of parent at token 200, which means all children that touch range (-150, 200]
    // the range will be (-150, 200], but sorted unsigned, so (0, 200] and (-150, 0) -> 0, 50, 100, 150, 200, -100, -50
    auto range = alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false);

    // let's start from 50 - this skips 0 and 50 as starting point is exclusive
    range.set_starting_position(starting_pos);

    // we should get 100, 150, 200, -100, -50
    auto range_sids = to_sids(range);
    BOOST_REQUIRE(range_sids == expected_result);
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_starting_pos_2) {
    auto parent_streams = sids({ -150, 200 });
    auto current_streams = sids({ -200, -150, -100, -50, 0, 50, 100, 150, 200, 250 });
    auto starting_pos = current_streams[2]; // -100, we need to pick it now, because find_children_range_from_parent_token will sort and move current_streams

    // we expect to get -50
    auto expected_result = sorted_vec(current_streams, 3, 4);

    // we search for children of parent at token 200, which means all children that touch range (-150, 200]
    // the range will be (-150, 200], but sorted unsigned, so (0, 200] and (-150, 0) -> 0, 50, 100, 150, 200, -100, -50
    auto range = alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false);

    // let's start from -100 - this leaves only -50 and skips everything before it
    range.set_starting_position(starting_pos);

    // we should get -50
    auto range_sids = to_sids(range);
    BOOST_REQUIRE(range_sids == expected_result);
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_starting_pos_3_wrap_around) {
    auto parent_streams = sids({ -50, 50 });
    auto current_streams = sids({ -200, -150, -100, -50, 0, 50, 100, 150, 200, 250 });
    auto starting_pos = current_streams[1]; // -150, we need to pick it now, because find_children_range_from_parent_token will sort and move current_streams

    // we expect to get -100, -50
    auto expected_result = sorted_vec(current_streams, 2, 4);

    // we search for children of parent at token -50, which means all children that touch range (-inf, -50] and (50, +inf) - wraps around
    // sorted unsigned -> (50, +inf) (-inf, -50] -> -> 50, 100, 150, 200, 250, -200, -150, -100, -50
    auto range = alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false);

    // let's start from -150 - this leaves only -100 and -50 and skips everything before it
    range.set_starting_position(starting_pos);

    // we should get -100, -50
    auto range_sids = to_sids(range);
    BOOST_REQUIRE(range_sids == expected_result);
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_starting_pos_4_wrap_around) {
    auto parent_streams = sids({ -50, 50 });
    auto current_streams = sids({ -200, -150, -100, -50, 0, 50, 100, 150, 200, 250 });
    auto starting_pos = current_streams[6]; // 100, we need to pick it now, because find_children_range_from_parent_token will sort and move current_streams

    // we expect to get 150, 200, 250, -200, -150, -100, -50
    auto expected_result = sorted_vec(current_streams, 7, 10, 0, 4);

    // we search for children of parent at token -50, which means all children that touch range (-inf, -50] and (50, +inf) - wraps around
    // sorted unsigned -> (50, +inf) (-inf, -50] -> -> 50, 100, 150, 200, 250, -200, -150, -100, -50
    auto range = alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false);

    // let's start from 100 - this leaves only 150, 200, 250, -200, -150, -100, -50
    range.set_starting_position(starting_pos);

    // we should get 150, 200, 250, -200, -150, -100, -50
    auto range_sids = to_sids(range);

    BOOST_REQUIRE(range_sids == expected_result);
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_1) {
    auto parent_streams = sids({ 0, 25, 50, 75 });
    auto current_streams = sids({ 25, 50, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[3], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_2) {
    auto parent_streams = sids({ 0, 25, 50, 75 });
    auto current_streams = sids({ 0, 50, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[3], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_3) {
    auto parent_streams = sids({ 0, 25, 50, 75 });
    auto current_streams = sids({ 0, 25, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[3], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_4) {
    auto parent_streams = sids({ 0, 25, 50, 75 });
    auto current_streams = sids({ 0, 25, 50 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[3], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_into_one_1) {
    auto parent_streams = sids({ 0, 25, 50 });
    auto current_streams = sids({ -50 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_into_one_2) {
    auto parent_streams = sids({ 0, 25, 50 });
    auto current_streams = sids({ 0 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_into_one_3) {
    auto parent_streams = sids({ 0, 25, 50 });
    auto current_streams = sids({ 10 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_into_one_4) {
    auto parent_streams = sids({ 0, 25, 50 });
    auto current_streams = sids({ 25 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_into_one_5) {
    auto parent_streams = sids({ 0, 25, 50 });
    auto current_streams = sids({ 50 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_merge_into_one_6) {
    auto parent_streams = sids({ 0, 25, 50 });
    auto current_streams = sids({ 110 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}


BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_1) {
    auto parent_streams = sids({ 0, 50, 100 });
    auto current_streams = sids({ -25, 0, 50, 100 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 3, 4));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_2) {
    auto parent_streams = sids({ 0, 50, 100 });
    auto current_streams = sids({ 0, 25, 50, 100 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 3));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 3, 4));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_3) {
    auto parent_streams = sids({ 0, 50, 100 });
    auto current_streams = sids({ 0, 50, 75, 100 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 4));
}

// Regression test for duplicate-token CHILD_SHARDS selection.
//
// Multiple current-generation stream ids can legitimately share the same token
// in CDC generation for small vnodes.  This test verifies that
// find_children_range_from_parent_token returns all such children rather than
// collapsing them to one.
//
// Setup: a static_sharder with 3 shards and ignore_msb=0 (one contiguous
// shard-pattern repetition across the ring).  We pick the tiny vnode at the
// very end of the ring — range (max-1, max] — which is so small that only
// shard 2 actually owns the single token inside it (last_token() maps to
// shard 2).  For shards 0 and 1, find_first_token_for_shard finds no token
// in the range and falls back to the vnode-end token (last_token()).  This is
// the standard CDC behaviour for shard slots without a token in a small vnode:
// their representative stream id uses the vnode end token.  The result is
// three distinct stream ids that all share the same token, which is exactly
// the scenario this regression test covers.
BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_duplicate_end_token_realistic) {

    auto prev_token = [] (dht::token t) {
        return dht::token::from_int64(dht::token::to_int64(t) - 1);
    };

    dht::static_sharder sharder(3, 0); // 3 shards, ignore_msb=0 → single shard-pattern repetition
    auto end = dht::last_token();
    auto start = prev_token(end);
    auto current_streams = stream_ids({
        cdc::stream_id(dht::find_first_token_for_shard(sharder, start, end, 0), 7),
        cdc::stream_id(dht::find_first_token_for_shard(sharder, start, end, 1), 7),
        cdc::stream_id(dht::find_first_token_for_shard(sharder, start, end, 2), 7),
    });

    BOOST_REQUIRE(current_streams.size() == 3);
    BOOST_REQUIRE_EQUAL(current_streams[0].token(), end);
    BOOST_REQUIRE_EQUAL(current_streams[1].token(), end);
    BOOST_REQUIRE_EQUAL(current_streams[2].token(), end);

    auto parent_streams = sids({ 0, std::numeric_limits<std::int64_t>::max() });

    // CHILD_SHARDS orders equal-token children by full stream id, so sort the
    // expected result the same way before comparing.
    auto expected = current_streams;
    std::sort(expected.begin(), expected.end(), [](const cdc::stream_id& a, const cdc::stream_id& b) {
        return compare_unsigned(a.to_bytes(), b.to_bytes()) < 0;
    });

    auto got = to_sids(alternator::find_children_range_from_parent_token(
            parent_streams,
            current_streams,
            parent_streams[1],
            false));

    BOOST_REQUIRE_MESSAGE(
            got == expected,
            format("The parent shard should include all {} current shard streams for this one-token vnode, but the selection returned only {} of them.",
                    expected.size(), got.size()));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_4) {
    auto parent_streams = sids({ 0, 50, 100 });
    auto current_streams = sids({ 0, 50, 100, 125 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1, 3, 4));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_from_one_1) {
    auto parent_streams = sids({ -10 });
    auto current_streams = sids({ 0, 50, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_from_one_2) {
    auto parent_streams = sids({ 0 });
    auto current_streams = sids({ 0, 50, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_from_one_3) {
    auto parent_streams = sids({ 25 });
    auto current_streams = sids({ 0, 50, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_from_one_4) {
    auto parent_streams = sids({ 50 });
    auto current_streams = sids({ 0, 50, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_from_one_5) {
    auto parent_streams = sids({ 60 });
    auto current_streams = sids({ 0, 50, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_from_one_6) {
    auto parent_streams = sids({ 75 });
    auto current_streams = sids({ 0, 50, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_from_one_7) {
    auto parent_streams = sids({ 100 });
    auto current_streams = sids({ 0, 50, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 3));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_and_merge_1) {
    auto parent_streams = sids({ 0, 50, 100 });
    auto current_streams = sids({ 25, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 2));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_and_merge_2) {
    auto parent_streams = sids({ -100, -50, 25, 50, 100, 200 });
    auto current_streams = sids({ 25, 75 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[2], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[3], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[4], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[5], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_and_merge_3) {
    auto parent_streams = sids({ -275, -75 });
    auto current_streams = sids({ -400, -300, -200, -100, -50, -10, 0, 10, 50, 100, 200, 300, 400 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 3, 4, 13));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 2, 5));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_and_merge_4) {
    auto parent_streams = sids({ 75, 275 });
    auto current_streams = sids({ -100, -50, -10, 0, 10, 50, 100, 200, 300, 400 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 7, 8, 10));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 6, 9));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_and_merge_5) {
    auto parent_streams = sids({ 0, 10 });
    auto current_streams = sids({ -20, -10 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 1));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_and_merge_6) {
    auto parent_streams = sids({ -20 });
    auto current_streams = sids({ -20, 0 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 2));
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_split_and_merge_7) {
    auto parent_streams = sids({ -20, -10 });
    auto current_streams = sids({ -20, 0 });

    auto range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[0], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 0, 2));

    range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[1], false));
    BOOST_REQUIRE(vec(range) == sorted_vec(current_streams, 1, 2));
}

namespace {
    struct encapsulated_range {
        const int from, to;

        explicit operator bool () const {
            return from < to;
        }
        encapsulated_range operator & (encapsulated_range other) const {
            auto f = std::max(from, other.from);
            auto t = std::min(to, other.to);
            if (f >= t) {
                return {0, 0};
            }
            return {f, t};
        }
        // friend std::ostream &operator << (std::ostream &o, const encapsulated_range &r) {
        //     o << "[" << r.from << ", " << r.to << ")";
        //     return o;
        // }
    };
}

BOOST_AUTO_TEST_CASE(test_find_children_range_from_parent_vnodes_brute_force_all_combinations) {
    constexpr int N = 8;
    constexpr int S = -(N / 2);
    std::vector<int> parent_streams_values, current_streams_values;
    utils::chunked_vector<cdc::stream_id> parent_streams, current_streams, expected;
    size_t prepare_iteration = std::numeric_limits<size_t>::max();

    auto get_encapsulated_range = [](const std::vector<int>& v, size_t index) -> std::pair<encapsulated_range, encapsulated_range>{
        auto end = v[index];
        if (index > 0) {
            auto start = v[index - 1];
            return { encapsulated_range{start, end} , encapsulated_range{end, end} };
        }
        auto start = v.back();
        return { encapsulated_range{start, std::numeric_limits<int>::max()}, encapsulated_range{std::numeric_limits<int>::min(), end} };
    };
    auto prepare = [&](size_t iteration) {
        if (prepare_iteration != iteration) {
            parent_streams_values.clear();
            current_streams_values.clear();
            parent_streams.clear();
            current_streams.clear();
            for(auto i = 0; i < N; ++i) {
                if (iteration & (1 << i)) {
                    parent_streams_values.push_back((S + i) * 10);
                    parent_streams.push_back(sid(parent_streams_values.back()));
                }
                if (iteration & (1 << (N + i))) {
                    current_streams_values.push_back((S + i) * 10);
                    current_streams.push_back(sid(current_streams_values.back()));
                }
            }
            prepare_iteration = iteration;
        }
    };

    auto run = [&](size_t iteration, size_t parent_index) {
        prepare(iteration);
        if (current_streams.empty() || parent_streams.empty()) {
            return;
        }
        // std::cout << "running iteration " << iteration << " parent_index " << parent_index << std::endl;
        // std::cout << "parents [";
        // for(auto v : parent_streams_values) {
        //     if (v != parent_streams_values.front()) {
        //         std::cout << ", ";
        //     }
        //     std::cout << v;
        // }
        // std::cout << "]" << std::endl;
        // std::cout << "currents [";
        // for(auto v : current_streams_values) {
        //     if (v != current_streams_values.front()) {
        //         std::cout << ", ";
        //     }
        //     std::cout << v;
        // }
        //std::cout << "]" << std::endl;
        auto [ range1, range2 ] = get_encapsulated_range(parent_streams_values, parent_index);

        expected.clear();
        for(auto i = 0u; i < current_streams_values.size(); ++i) {
            auto [ range3, range4 ] = get_encapsulated_range(current_streams_values, i);

            if (range1 & range3 || range1 & range4 || range2 & range3 || range2 & range4) {
                expected.push_back(current_streams[i]);
                // std::cout << "range1 " << range1 << " range2 " << range2 << " range3 " << range3 << " range4 " << range4 << " range1 & range3 " << (range1 & range3) << " range1 & range4 " << (range1 & range4) << " range2 & range3 " << (range2 & range3) << " range2 & range4 " << (range2 & range4) << std::endl;
            }
        }
        std::sort(expected.begin(), expected.end(), [](const cdc::stream_id &a, const cdc::stream_id &b) {
            return compare_unsigned(a.to_bytes(), b.to_bytes()) < 0;
        });
        BOOST_REQUIRE(!expected.empty());

        auto old_current_streams = current_streams;
        auto old_parent_streams = parent_streams;
        utils::chunked_vector<cdc::stream_id> range;
        try {
            range = to_sids(alternator::find_children_range_from_parent_token(parent_streams, current_streams, parent_streams[parent_index], false));
        }
        catch(...) {

            BOOST_REQUIRE_MESSAGE(false,
                                "iteration " << iteration << " parent_index " << parent_index
                            );
        }
        auto produced = vec(range);

        if (produced != expected) {
            std::cout << "produced " << produced << std::endl;
            std::cout << "expected " << expected << std::endl;

            BOOST_REQUIRE_MESSAGE(produced == expected,
                                "produced " << produced << "\n" <<
                                "expected " << expected << "\n" <<
                                "iteration " << iteration << " parent_index " << parent_index
                            );
        }

        std::sort(parent_streams.begin(), parent_streams.end(), [](const cdc::stream_id &a, const cdc::stream_id &b) {
            return a.token() < b.token();
        });
        std::sort(current_streams.begin(), current_streams.end(), [](const cdc::stream_id &a, const cdc::stream_id &b) {
            return a.token() < b.token();
        });
        BOOST_REQUIRE_MESSAGE(parent_streams == old_parent_streams,
                            "parent streams modified!\n" <<
                            "produced " << parent_streams << "\n" <<
                            "expected " << old_parent_streams << "\n" <<
                            "iteration " << iteration << " parent_index " << parent_index
                        );
        BOOST_REQUIRE_MESSAGE(current_streams == old_current_streams,
                            "current streams modified!\n" <<
                            "produced " << current_streams << "\n" <<
                            "expected " << old_current_streams << "\n" <<
                            "iteration " << iteration << " parent_index " << parent_index
                        );
    };

    // run(2310, 0); // if you need to debug a specific case
    for(auto iteration = 0u; iteration < (1 << (2 * N)); ++iteration) {
        prepare(iteration);
        for(auto parent_index = 0u; parent_index < parent_streams.size(); ++parent_index) {
            run(iteration, parent_index);
        }
    }
}

// A randomized round-trip test for alternator's response compressor (zlib_compressor).
//
// Modelled on test/boost/stream_compressor_test.cc's randomized test: it knows nothing
// about the compressor's internals, it only generates random inputs and parameters,
// pushes them through the compressor, and checks that zlib's inflate() gets back what
// went in. Its real target, though, is not the round trip itself but the buffer-overrun
// guard inside zlib_compressor: the interesting cases are those where the last chunk's
// output buffer is sized down below compressed_buffer_size.
//
// All of its bug-finding power comes from the number and the variety of the cases it
// covers, so it is meant to be run in a loop, for as long as one can afford, rather
// than once. The whole run is derived from the test framework's random seed, which the
// test runner prints as `random-seed=<N>`, so a failed run can be replayed with
// `--random-seed=<N>`; each individual case is described by a 64-bit seed reported on
// failure.

// Draws a random integer from [lo, hi], with a distribution meant to hit boundary
// conditions much more often than a uniform draw would: every bit width gets a similar
// share of the probability space, and powers of two, their neighbours and the ends of
// the range get an extra boost.
static uint64_t draw_interesting(std::mt19937_64& rng, uint64_t lo, uint64_t hi) {
    BOOST_REQUIRE_LE(lo, hi);
    auto uniform = [&rng] (uint64_t a, uint64_t b) {
        return std::uniform_int_distribution<uint64_t>(a, b)(rng);
    };
    // One in three draws lands on a "special" value.
    if (uniform(0, 2) == 0) {
        utils::small_vector<uint64_t, 64> candidates;
        auto add = [&] (int64_t v) {
            if (v >= int64_t(lo) && v <= int64_t(hi)) {
                candidates.push_back(uint64_t(v));
            }
        };
        add(int64_t(lo));
        add(int64_t(lo) + 1);
        add(int64_t(hi) - 1);
        add(int64_t(hi));
        for (int k = 0; k < 63 && (uint64_t(1) << k) <= hi + 2; ++k) {
            for (int64_t delta = -2; delta <= 2; ++delta) {
                add(int64_t(uint64_t(1) << k) + delta);
            }
        }
        if (!candidates.empty()) {
            return candidates[uniform(0, candidates.size() - 1)];
        }
    }
    // Otherwise: draw a bit width uniformly, then draw uniformly within that bit width.
    const int width = int(uniform(std::bit_width(lo), std::bit_width(hi)));
    const uint64_t band_lo = width ? (uint64_t(1) << (width - 1)) : 0;
    const uint64_t band_hi = width ? ((uint64_t(1) << width) - 1) : 0;
    return uniform(std::max(lo, band_lo), std::min(hi, band_hi));
}

// A string of `size` bytes, each drawn from the first `alphabet_size` byte values.
// The alphabet size is a cheap knob for the compressibility of the data: a 1-letter
// alphabet compresses to nothing, a 256-letter alphabet is incompressible.
static std::string draw_message(std::mt19937_64& rng, size_t size, unsigned alphabet_size) {
    auto b = std::string(size, '\0');
    for (size_t i = 0; i < size; i += 8) {
        uint64_t r = rng();
        for (size_t k = i, end = std::min(i + 8, size); k < end; ++k) {
            b[k] = static_cast<char>((alphabet_size * (r & 0xff)) >> 8);
            r >>= 8;
        }
    }
    return b;
}

// Decompresses with zlib, in the matching (gzip or raw zlib) format.
static std::string zlib_decompress(bool gzip, std::string_view compressed) {
    z_stream zs;
    memset(&zs, 0, sizeof(zs));
    BOOST_REQUIRE_EQUAL(inflateInit2(&zs, (gzip ? 16 : 0) + MAX_WBITS), Z_OK);
    auto cleanup = seastar::defer([&zs] () noexcept { inflateEnd(&zs); });
    zs.next_in = reinterpret_cast<unsigned char*>(const_cast<char*>(compressed.data()));
    zs.avail_in = compressed.size();
    std::string out;
    std::string buf(64 * 1024, '\0');
    int e;
    do {
        zs.next_out = reinterpret_cast<unsigned char*>(buf.data());
        zs.avail_out = buf.size();
        e = inflate(&zs, Z_NO_FLUSH);
        BOOST_REQUIRE_MESSAGE(e >= Z_OK, fmt::format("inflate() failed: {}", e));
        out.append(buf.data(), buf.size() - zs.avail_out);
    } while (e != Z_STREAM_END && (zs.avail_in > 0 || zs.avail_out == 0));
    BOOST_REQUIRE_EQUAL(e, Z_STREAM_END);
    return out;
}

static void run_zlib_roundtrip_case(uint64_t case_seed) {
    constexpr size_t max_message_size = 512 * 1024;

    std::mt19937_64 rng(case_seed);
    const bool gzip = rng() % 2;
    const int level = draw_interesting(rng, 1, 9);
    const size_t message_size = draw_interesting(rng, 0, max_message_size);
    const unsigned alphabet_size = draw_interesting(rng, 1, 256);
    // The compressor's output buffer is 1024 bytes, and the buggy sizing of the
    // last chunk's buffer depends on how much state deflate() is sitting on when the
    // stream is closed, so chunk sizes around that scale are the interesting ones.
    const size_t max_chunk_size = draw_interesting(rng, 1, std::max<size_t>(message_size, 1));

    const auto message = draw_message(rng, message_size, alphabet_size);
    std::vector<std::string> chunks;
    for (size_t pos = 0; pos < message.size(); ) {
        const size_t n = std::min(message.size() - pos, size_t(draw_interesting(rng, 1, max_chunk_size)));
        chunks.emplace_back(message.substr(pos, n));
        pos += n;
    }

    const auto description = fmt::format(
            "case_seed={} gzip={} level={} message_size={} alphabet_size={} max_chunk_size={} chunks={}",
            case_seed, gzip, level, message_size, alphabet_size, max_chunk_size, chunks.size());

    // The compressor aborts (rather than throws) when its internal overrun guard fires,
    // so the only way to learn which case did it is to announce cases up front.
    if (::getenv("SCYLLA_ZLIB_TRACE_CASES")) {
        fmt::print(stderr, "case: {}\n", description);
    }

    std::string compressed;
    try {
        alternator::compress_chunks_for_test(gzip, level, chunks,
                [&compressed] (temporary_buffer<char>&& buf) {
                    // The buffer must be consumed (moved out of), like the production
                    // write functions do - the compressor reuses _output_buf otherwise.
                    auto b = std::move(buf);
                    compressed.append(b.get(), b.size());
                    return make_ready_future<>();
                }).get();
    } catch (...) {
        // Without this, the case parameters would be lost, and the failure unreproducible.
        BOOST_FAIL(fmt::format("compression threw: {} exception={}", description, std::current_exception()));
    }

    const auto decompressed = zlib_decompress(gzip, compressed);
    if (decompressed != message) {
        size_t i = 0;
        while (i < std::min(message.size(), decompressed.size()) && message[i] == decompressed[i]) {
            ++i;
        }
        BOOST_FAIL(fmt::format("round trip mismatch: {} compressed_size={} decompressed_size={} first_difference_at={}",
                description, compressed.size(), decompressed.size(), i));
    }
}

// A hardcoded reproducer for a heap buffer overflow in zlib_compressor.
//
// zlib_compressor sizes the output buffer for the final Z_FINISH round from
// deflatePending() + deflateBound(avail_in) + 1, but then tells deflate() that it has
// compressed_buffer_size (1024) bytes of room, regardless of how big the buffer really
// is. Neither deflatePending() nor deflateBound() accounts for input that deflate has
// already consumed but not yet emitted, so once the stream is part-way through, the
// computed size can be far below what deflate() still has to write - and deflate(),
// trusting avail_out, writes past the end of the allocation.
//
// This needs no chunking: a single compress(..., is_last_chunk=true) call, i.e. the
// plain (non-chunked) response path, is enough, because the buffer is re-sized on every
// iteration of the loop and only the first of those sizings is one deflateBound() can
// vouch for.
//
// Under ASAN (a debug build) this aborts with a heap-buffer-overflow inside deflate():
//   WRITE of size 808 ... is located 0 bytes after 242-byte region
//
// The payload is 17408 bytes of random data. The size matters: it is just over zlib's
// 16384-symbol buffer (memLevel 8), and messages at or below 16 KiB never built up
// enough unflushed state to trigger this (36000 randomized attempts at 16 KiB, none at
// 17 KiB survived more than a few dozen).
static constexpr std::string_view overrun_repro_message_base64 =
    "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";

SEASTAR_THREAD_TEST_CASE(test_zlib_compressor_output_buffer_overrun) {
    constexpr bool gzip = true;
    constexpr int level = 5;

    const bytes decoded = base64_decode(overrun_repro_message_base64);
    const std::string message(reinterpret_cast<const char*>(decoded.data()), decoded.size());
    BOOST_REQUIRE_EQUAL(message.size(), 17408);

    std::string compressed;
    alternator::compress_message_for_test(gzip, level, message,
            [&compressed] (temporary_buffer<char>&& buf) {
                auto b = std::move(buf);
                compressed.append(b.get(), b.size());
                return make_ready_future<>();
            }).get();
    BOOST_REQUIRE_EQUAL(zlib_decompress(gzip, compressed), message);
}

SEASTAR_THREAD_TEST_CASE(test_zlib_roundtrip_randomized) {
    // How long a single run of this test case is allowed to take. It's deliberately
    // short: the bug-finding power of this test comes from running it many times over
    // (in parallel, and/or in a loop) in a dedicated session.
    // SCYLLA_ZLIB_BUDGET_MS raises it for a dedicated fuzzing session.
    auto time_budget = std::chrono::milliseconds(200);
    if (const char* budget = ::getenv("SCYLLA_ZLIB_BUDGET_MS")) {
        time_budget = std::chrono::milliseconds(std::stoull(budget));
    }

    // A single case can be a lot of uninterruptible compression work, which takes long
    // enough to trip the stall detector. That's inherent to what this test does, so we
    // mute the reports instead of drowning the log in them.
    const auto prev_notify_ms = seastar::engine().get_blocked_reactor_notify_ms();
    seastar::engine().update_blocked_reactor_notify_ms(std::chrono::hours(1));
    const auto restore_notify_ms = seastar::defer([prev_notify_ms] () noexcept {
        seastar::engine().update_blocked_reactor_notify_ms(prev_notify_ms);
    });

    // For debugging: SCYLLA_ZLIB_CASE_SEED=<N> replays a single reported case.
    if (const char* seed = ::getenv("SCYLLA_ZLIB_CASE_SEED")) {
        run_zlib_roundtrip_case(std::stoull(seed));
        return;
    }

    std::mt19937_64 master(tests::random::get_int<uint64_t>());
    const auto deadline = std::chrono::steady_clock::now() + time_budget;
    uint64_t cases = 0;
    do {
        run_zlib_roundtrip_case(master());
        ++cases;
        seastar::thread::maybe_yield();
    } while (std::chrono::steady_clock::now() < deadline);
    testlog.info("zlib_roundtrip_randomized: cases_tested={}", cases);
}
