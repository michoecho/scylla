/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <seastar/core/sstring.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/align.hh>
#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/short_streams.hh>
#include <seastar/util/closeable.hh>

#include "sstables/checksum_utils.hh"
#include "sstables/compression_info_cache.hh"
#include "sstables/generation_type.hh"
#include "sstables/sstables.hh"
#include "sstables/key.hh"
#include "sstables/open_info.hh"
#include "sstables/version.hh"
#include "test/lib/exception_utils.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/random_schema.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/random_utils.hh"
#include "test/lib/reader_concurrency_semaphore.hh"
#include "test/lib/scylla_test_case.hh"
#include "test/lib/test_utils.hh"
#include "schema/schema.hh"
#include "sstables/compressor.hh"
#include "replica/database.hh"
#include "test/boost/sstable_test.hh"
#include "test/lib/tmpdir.hh"
#include "partition_slice_builder.hh"
#include "sstables/sstable_mutation_reader.hh"
#include "sstables/binary_search.hh"
#include "sstables/exceptions.hh"
#include "sstables/file_writer.hh"
#include "sstables/writer.hh"

#include <boost/range/combine.hpp>

using namespace sstables;

bytes as_bytes(const sstring& s) {
    return { reinterpret_cast<const int8_t*>(s.data()), s.size() };
}

future<> test_using_working_sst(schema_ptr s, sstring dir) {
    return test_env::do_with_async([s = std::move(s), dir = std::move(dir)] (test_env& env) {
        (void)env.reusable_sst(std::move(s), std::move(dir)).get();
    });
}

SEASTAR_TEST_CASE(uncompressed_data) {
    return test_using_working_sst(uncompressed_schema(), uncompressed_dir());
}

static auto make_schema_for_compressed_sstable() {
    return schema_builder(this_smp_shard_count(), "ks", "cf").with_column("pk", utf8_type, column_kind::partition_key).build();
}

SEASTAR_TEST_CASE(compressed_data) {
    auto s = make_schema_for_compressed_sstable();
    return test_using_working_sst(std::move(s), "test/resource/sstables/compressed");
}

SEASTAR_TEST_CASE(composite_index) {
    return test_using_working_sst(composite_schema(), "test/resource/sstables/composite");
}

template<std::invocable<test_env&, sstable_ptr> Func>
inline future<std::invoke_result_t<Func, test_env&, sstable_ptr>>
test_using_reusable_sst(schema_ptr s, sstring dir, sstables::generation_type::int_t gen, Func&& func) {
    using ret_type = std::invoke_result_t<Func, test_env&, sstable_ptr>;
    return test_env::do_with_async_returning<ret_type>([s = std::move(s), dir = std::move(dir), gen, func = std::move(func)] (test_env& env) {
        auto sst = env.reusable_sst(std::move(s), std::move(dir), generation_from_value(gen)).get();
        return func(env, std::move(sst));
    });
}

future<std::vector<partition_key>> index_read(schema_ptr schema, sstring path) {
    return test_using_reusable_sst(std::move(schema), std::move(path), 1, [] (test_env& env, sstable_ptr ptr) {
        auto indexes = sstables::test(ptr).read_indexes(env.make_reader_permit()).get();
        return indexes | std::views::transform([] (const sstables::test::index_entry& e) { return e.key; }) | std::ranges::to<std::vector<partition_key>>();
    });
}

SEASTAR_TEST_CASE(simple_index_read) {
    auto vec = co_await index_read(uncompressed_schema(), uncompressed_dir());
    BOOST_REQUIRE(vec.size() == 4);
}

SEASTAR_TEST_CASE(composite_index_read) {
    auto vec = co_await index_read(composite_schema(), "test/resource/sstables/composite");
    BOOST_REQUIRE(vec.size() == 20);
}

template<uint64_t Position, uint64_t EntryPosition, uint64_t EntryKeySize>
future<> summary_query(schema_ptr schema, sstring path, sstables::generation_type::int_t generation) {
    return test_using_reusable_sst(std::move(schema), path, generation, [] (test_env& env, sstable_ptr ptr) {
        auto entry = sstables::test(ptr).read_summary_entry(Position).get();
        BOOST_REQUIRE(entry.position == EntryPosition);
        BOOST_REQUIRE(entry.key.size() == EntryKeySize);
    });
}

template<uint64_t Position, uint64_t EntryPosition, uint64_t EntryKeySize>
future<> summary_query_fail(schema_ptr schema, sstring path, sstables::generation_type::int_t generation) {
    try {
        co_await summary_query<Position, EntryPosition, EntryKeySize>(std::move(schema), std::move(path), generation);
    } catch (const std::out_of_range&) {
    }
}

SEASTAR_TEST_CASE(small_summary_query_ok) {
    return summary_query<0, 0, 5>(uncompressed_schema(), uncompressed_dir(), 1);
}

SEASTAR_TEST_CASE(small_summary_query_fail) {
    return summary_query_fail<2, 0, 5>(uncompressed_schema(), uncompressed_dir(), 1);
}

SEASTAR_TEST_CASE(small_summary_query_negative_fail) {
    return summary_query_fail<-uint64_t(2), 0, 5>(uncompressed_schema(), uncompressed_dir(), 1);
}

SEASTAR_TEST_CASE(big_summary_query_0) {
    return summary_query<0, 0, 182>(uncompressed_schema(), "test/resource/sstables/bigsummary", 76);
}

SEASTAR_TEST_CASE(big_summary_query_32) {
    return summary_query<32, 0xc4000, 182>(uncompressed_schema(), "test/resource/sstables/bigsummary", 76);
}

// The following two files are just a copy of uncompressed's 1. But the Summary
// is removed (and removed from the TOC as well). We should reconstruct it
// in this case, so the queries should still go through
SEASTAR_TEST_CASE(missing_summary_query_ok) {
    return summary_query<0, 0, 5>(uncompressed_schema(), uncompressed_dir(), 2);
}

SEASTAR_TEST_CASE(missing_summary_query_fail) {
    return summary_query_fail<2, 0, 5>(uncompressed_schema(), uncompressed_dir(), 2);
}

SEASTAR_TEST_CASE(missing_summary_query_negative_fail) {
    return summary_query_fail<-uint64_t(2), 0, 5>(uncompressed_schema(), uncompressed_dir(), 2);
}

// TODO: only one interval is generated with size-based sampling. Test it with a sstable that will actually result
// in two intervals.
#if 0
SEASTAR_TEST_CASE(missing_summary_interval_1_query_ok) {
    return summary_query<1, 19, 6>(uncompressed_schema(1), uncompressed_dir(), 2);
}
#endif

SEASTAR_TEST_CASE(missing_summary_first_last_sane) {
    return test_using_reusable_sst(uncompressed_schema(), uncompressed_dir(), 2, [] (test_env& env, shared_sstable ptr) {
        const auto& summary = ptr->get_summary();
        BOOST_REQUIRE(summary.header.size == 1);
        BOOST_REQUIRE(summary.positions.size() == 1);
        BOOST_REQUIRE(summary.entries.size() == 1);
        BOOST_REQUIRE(bytes_view(summary.first_key) == as_bytes("vinna"));
        BOOST_REQUIRE(bytes_view(summary.last_key) == as_bytes("finna"));
    });
}

static future<std::pair<sstable_ptr, sstable_ptr>> do_write_sst(test_env& env, schema_ptr schema, sstring load_dir, sstring write_dir, sstables::generation_type generation) {
    auto sst = co_await env.reusable_sst(std::move(schema), load_dir, generation);
    sstable_generation_generator gen;
    auto sst2 = co_await sstables::test(sst).store(write_dir, gen());
    co_return std::make_pair(sst, sst2);
}

static future<std::pair<sstables::generation_type, sstables::generation_type>> write_sst_info(schema_ptr schema, sstring load_dir, sstring write_dir, sstables::generation_type generation) {
    std::pair<sstables::generation_type, sstables::generation_type> ret;
    co_await test_env::do_with_async([schema = std::move(schema), load_dir = std::move(load_dir), write_dir = std::move(write_dir),
                                    generation = std::move(generation), &ret] (test_env& env) {
        auto [sst1, sst2] = do_write_sst(env, std::move(schema), std::move(load_dir), std::move(write_dir), std::move(generation)).get();
        ret = std::make_pair(sst1->generation(), sst2->generation());
    // sstables::test::store() writes the components of an already loaded sstable back
    // out, which for CompressionInfo.db requires the in-memory copy of the chunk
    // offsets -- the copy which the evictable cache does away with.
    }, {.compressioninfo_is_evictable = false});
    co_return ret;
}

static future<> check_component_integrity(component_type component) {
    tmpdir tmp;
    auto load_gen = sstables::generation_type(1);
    auto [gen1, gen2] = co_await write_sst_info(make_schema_for_compressed_sstable(), "test/resource/sstables/compressed", tmp.path().string(), load_gen);
    auto file_path_a = sstable::filename("test/resource/sstables/compressed", "ks", "cf", la, load_gen, big, component);
    auto file_path_b = sstable::filename(tmp.path().string(), "ks", "cf", la, gen2, big, component);
    auto eq = co_await tests::compare_files(file_path_a, file_path_b);
    BOOST_REQUIRE(eq);
}

SEASTAR_TEST_CASE(check_compressed_info_func) {
    return check_component_integrity(component_type::CompressionInfo);
}

future<>
write_and_validate_sst(schema_ptr s, sstring dir, sstables::generation_type load_gen, noncopyable_function<void (shared_sstable sst1, shared_sstable sst2)> func) {
    return test_env::do_with_async([s = std::move(s), dir = std::move(dir), load_gen, func = std::move(func)] (test_env& env) mutable {
        auto [sst1, sst2] = do_write_sst(env, s, dir, env.tempdir().path().native(), load_gen).get();
        func(std::move(sst1), std::move(sst2));
    // See the comment in write_sst_info().
    }, {.compressioninfo_is_evictable = false});
}

SEASTAR_TEST_CASE(check_summary_func) {
    auto s = make_schema_for_compressed_sstable();
    return write_and_validate_sst(std::move(s), "test/resource/sstables/compressed", sstables::generation_type(1), [] (shared_sstable sst1, shared_sstable sst2) {
        sstables::test(sst2).read_summary().get();

        const summary& sst1_s = sst1->get_summary();
        const summary& sst2_s = sst2->get_summary();

        BOOST_REQUIRE(::memcmp(&sst1_s.header, &sst2_s.header, sizeof(summary::header)) == 0);
        BOOST_REQUIRE(sst1_s.positions == sst2_s.positions);
        BOOST_REQUIRE(sst1_s.entries == sst2_s.entries);
        BOOST_REQUIRE(sst1_s.first_key.value == sst2_s.first_key.value);
        BOOST_REQUIRE(sst1_s.last_key.value == sst2_s.last_key.value);
    });
}

SEASTAR_TEST_CASE(check_filter_func) {
    return check_component_integrity(component_type::Filter);
}

SEASTAR_TEST_CASE(check_statistics_func) {
    auto s = make_schema_for_compressed_sstable();
    return write_and_validate_sst(std::move(s), "test/resource/sstables/compressed", sstables::generation_type(1), [] (shared_sstable sst1, shared_sstable sst2) {
        sstables::test(sst2).read_statistics().get();
        const auto& sst1_s = sst1->get_statistics();
        const auto& sst2_s = sst2->get_statistics();

        BOOST_REQUIRE(sst1_s.offsets.elements.size() == sst2_s.offsets.elements.size());
        BOOST_REQUIRE(sst1_s.contents.size() == sst2_s.contents.size());

        for (auto&& e : boost::combine(sst1_s.offsets.elements, sst2_s.offsets.elements)) {
            BOOST_REQUIRE(boost::get<0>(e).second ==  boost::get<1>(e).second);
        }
        // TODO: compare the field contents from both sstables.
    });
}

SEASTAR_TEST_CASE(check_toc_func) {
    auto s = make_schema_for_compressed_sstable();
    return write_and_validate_sst(std::move(s), "test/resource/sstables/compressed", sstables::generation_type(1), [] (shared_sstable sst1, shared_sstable sst2) {
        sstables::test(sst2).read_toc().get();
        auto& sst1_c = sstables::test(sst1).get_components();
        auto& sst2_c = sstables::test(sst2).get_components();

        BOOST_REQUIRE(sst1_c == sst2_c);
    });
}

SEASTAR_TEST_CASE(uncompressed_random_access_read) {
    return test_using_reusable_sst(uncompressed_schema(), uncompressed_dir(), 1, [] (auto& env, auto sstp) {
        temporary_buffer<char> buf = sstp->data_read(97, 6, env.make_reader_permit()).get();
        BOOST_REQUIRE(sstring(buf.get(), buf.size()) == "gustaf");
    });
}

SEASTAR_TEST_CASE(compressed_random_access_read) {
    auto s = make_schema_for_compressed_sstable();
    return test_using_reusable_sst(std::move(s), "test/resource/sstables/compressed", 1, [] (auto& env, auto sstp) {
        temporary_buffer<char> buf = sstp->data_read(97, 6, env.make_reader_permit()).get();
        BOOST_REQUIRE(sstring(buf.get(), buf.size()) == "gustaf");
    });
}


SEASTAR_TEST_CASE(find_key_map) {
    return test_using_reusable_sst(map_schema(), "test/resource/sstables/map_pk", 1, [] (auto& env, auto sstp) {
        schema_ptr s = map_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<data_value> kk;

        auto b1 = to_bytes("2");
        auto b2 = to_bytes("2");

        auto map_type = map_type_impl::get_instance(bytes_type, bytes_type, true);
        auto map_element = std::make_pair<data_value, data_value>(data_value(b1), data_value(b2));
        std::vector<std::pair<data_value, data_value>> map;
        map.push_back(map_element);

        kk.push_back(make_map_value(map_type, map));

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        BOOST_REQUIRE(sstables::binary_search(s->get_partitioner(), summary.entries, key) == 0);
    });
}

SEASTAR_TEST_CASE(find_key_set) {
    return test_using_reusable_sst(set_schema(), "test/resource/sstables/set_pk", 1, [] (auto& env, auto sstp) {
        schema_ptr s = set_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<data_value> kk;

        std::vector<data_value> set;

        bytes b1("1");
        bytes b2("2");

        set.push_back(data_value(b1));
        set.push_back(data_value(b2));
        auto set_type = set_type_impl::get_instance(bytes_type, true);
        kk.push_back(make_set_value(set_type, set));

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        BOOST_REQUIRE(sstables::binary_search(s->get_partitioner(), summary.entries, key) == 0);
    });
}

SEASTAR_TEST_CASE(find_key_list) {
    return test_using_reusable_sst(list_schema(), "test/resource/sstables/list_pk", 1, [] (auto& env, auto sstp) {
        schema_ptr s = set_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<data_value> kk;

        std::vector<data_value> list;

        bytes b1("1");
        bytes b2("2");
        list.push_back(data_value(b1));
        list.push_back(data_value(b2));

        auto list_type = list_type_impl::get_instance(bytes_type, true);
        kk.push_back(make_list_value(list_type, list));

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        BOOST_REQUIRE(sstables::binary_search(s->get_partitioner(), summary.entries, key) == 0);
    });
}


SEASTAR_TEST_CASE(find_key_composite) {
    return test_using_reusable_sst(composite_schema(), "test/resource/sstables/composite", 1, [] (auto& env, auto sstp) {
        schema_ptr s = composite_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<data_value> kk;

        auto b1 = bytes("HCG8Ee7ENWqfCXipk4-Ygi2hzrbfHC8pTtH3tEmV3d9p2w8gJPuMN_-wp1ejLRf4kNEPEgtgdHXa6NoFE7qUig==");
        auto b2 = bytes("VJizqYxC35YpLaPEJNt_4vhbmKJxAg54xbiF1UkL_9KQkqghVvq34rZ6Lm8eRTi7JNJCXcH6-WtNUSFJXCOfdg==");

        kk.push_back(data_value(b1));
        kk.push_back(data_value(b2));

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        BOOST_REQUIRE(sstables::binary_search(s->get_partitioner(), summary.entries, key) == 0);
    });
}

SEASTAR_TEST_CASE(all_in_place) {
    return test_using_reusable_sst(uncompressed_schema(), "test/resource/sstables/bigsummary", 76, [] (auto& env, auto sstp) {
        auto& summary = sstables::test(sstp)._summary();

        int idx = 0;
        for (auto& e: summary.entries) {
            auto key = sstables::key::from_bytes(bytes(e.key));
            BOOST_REQUIRE(sstables::binary_search(sstp->get_schema()->get_partitioner(), summary.entries, key) == idx++);
        }
    });
}

SEASTAR_TEST_CASE(full_index_search) {
    return test_using_reusable_sst(uncompressed_schema(), uncompressed_dir(), 1, [] (auto& env, auto sstp) {
        auto index_list = sstables::test(sstp).read_indexes(env.make_reader_permit()).get();
        int idx = 0;
        for (auto& e : index_list) {
            auto key = key::from_partition_key(*sstp->get_schema(), e.key);
            BOOST_REQUIRE(sstables::binary_search(sstp->get_schema()->get_partitioner(), index_list, key) == idx++);
        }
    });
}

SEASTAR_TEST_CASE(not_find_key_composite_bucket0) {
    return test_using_reusable_sst(composite_schema(), "test/resource/sstables/composite", 1, [] (auto& env, auto sstp) {
        schema_ptr s = composite_schema();
        auto& summary = sstables::test(sstp)._summary();
        std::vector<data_value> kk;

        auto b1 = bytes("ZEunFCoqAidHOrPiU3U6UAvUU01IYGvT3kYtYItJ1ODTk7FOsEAD-dqmzmFNfTDYvngzkZwKrLxthB7ItLZ4HQ==");
        auto b2 = bytes("K-GpWx-QtyzLb12z5oNS0C03d3OzNyBKdYJh1XjHiC53KudoqdoFutHUMFLe6H9Emqv_fhwIJEKEb5Csn72f9A==");

        kk.push_back(data_value(b1));
        kk.push_back(data_value(b2));

        auto key = sstables::key::from_deeply_exploded(*s, kk);
        // (result + 1) * -1 -1 = 0
        BOOST_REQUIRE(sstables::binary_search(s->get_partitioner(), summary.entries, key) == -2);
    });
}

// See CASSANDRA-7593. This sstable writes 0 in the range_start. We need to handle that case as well
SEASTAR_TEST_CASE(wrong_range) {
    return test_using_reusable_sst(uncompressed_schema(), "test/resource/sstables/wrongrange", 114, [] (auto& env, auto sstp) {
        auto range = dht::partition_range::make_singular(make_dkey(uncompressed_schema(), "todata"));
        auto s = columns_schema();
        auto rd = sstp->make_reader(s, env.make_reader_permit(), range, s->full_slice());
        auto close_rd = deferred_close(rd);
        (void)read_mutation_from_mutation_reader(rd).get();
    });
}

future<sstable_ptr> mutate_sstable_level(test_env& env, sstable_ptr sstp, const std::string& dir_path, uint32_t new_level, sstables::update_sstable_id update_id = sstables::update_sstable_id::no) {
    auto modifier = [new_level] (sstables::sstable& sst) {
        sst.mutate_sstable_level(new_level);
    };
    auto creator = [&env, &dir_path] (shared_sstable sstp) {
        return env.make_sstable(sstp->get_schema(), dir_path, sstp->get_version());
    };

    auto new_sst = co_await sstp->link_with_rewritten_component(std::move(creator), component_type::Statistics, modifier, update_id);
    co_await sstp->unlink();
    sstp = new_sst;

    sstp = co_await env.reusable_sst(uncompressed_schema(), dir_path, sstp->generation());
    co_return sstp;
}

SEASTAR_TEST_CASE(statistics_rewrite) {
    return test_env::do_with_async([] (test_env& env) {
        auto random_spec = tests::make_random_schema_specification(
            "ks",
            std::uniform_int_distribution<size_t>(1, 4),
            std::uniform_int_distribution<size_t>(2, 4),
            std::uniform_int_distribution<size_t>(2, 8),
            std::uniform_int_distribution<size_t>(2, 8));
        auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};
        auto schema = random_schema.schema();

        const auto muts = tests::generate_random_mutations(random_schema, 2).get();
        auto sstp = make_sstable_containing(env.make_sstable(schema, sstable::version_types::me), muts).get();

        auto toc_path = fmt::to_string(sstp->toc_filename());
        auto dir_path = std::filesystem::path(toc_path).parent_path().string();

        BOOST_REQUIRE(sstp->get_sstable_level() != 10);

        sstp = mutate_sstable_level(env, sstp, dir_path, 10).get();
        sstp = env.reusable_sst(schema, dir_path, sstp->generation()).get();
        BOOST_REQUIRE(sstp->get_sstable_level() == 10);
    });
}

// Tests for reading a large partition for which the index contains a
// "promoted index", i.e., a sample of the column names inside the partition,
// with which we can avoid reading the entire partition when we look only
// for a specific subset of columns. The test sstable for the read test was
// generated in Cassandra.

static schema_ptr large_partition_schema() {
    static thread_local auto s = [] {
        schema_builder builder(this_smp_shard_count(), "try1", "data", generate_legacy_id("try1", "data"));
        builder.with_column("t1", utf8_type, column_kind::partition_key);
        builder.with_column("t2", utf8_type, column_kind::clustering_key);
        builder.with_column("t3", utf8_type);
        return builder.build(schema_builder::compact_storage::no);
    }();
    return s;
}

static future<shared_sstable> load_large_partition_sst(test_env& env, const sstables::sstable::version_types version) {
    auto s = large_partition_schema();
    auto dir = get_test_dir("large_partition", s);
    return env.reusable_sst(std::move(s), std::move(dir), 3, version);
}

// This is a rudimentary test that reads an sstable exported from Cassandra
// which contains a promoted index. It just checks that the promoted index
// is read from disk, as an unparsed array, and doesn't actually use it to
// search for anything.
SEASTAR_TEST_CASE(promoted_index_read) {
  return for_each_sstable_version([] (const sstables::sstable::version_types version) {
    if (!has_summary_and_index(version)) {
        // This test is so basic that updating it to support `ms` sstables is not worth the effort.
        return make_ready_future<>();
    }
    return test_env::do_with_async([version] (test_env& env) {
        auto sstp = load_large_partition_sst(env, version).get();
        std::vector<sstables::test::index_entry> vec = sstables::test(sstp).read_indexes(env.make_reader_permit()).get();
        BOOST_REQUIRE(vec.size() == 1);
        BOOST_REQUIRE(vec[0].promoted_index_size > 0);
    });
  });
}

// Use an empty string for ck1, ck2, or both, for unbounded ranges.
static query::partition_slice make_partition_slice(const schema& s, sstring ck1, sstring ck2) {
    std::optional<query::clustering_range::bound> b1;
    if (!ck1.empty()) {
        b1.emplace(clustering_key_prefix::from_single_value(
                s, utf8_type->decompose(ck1)));
    }
    std::optional<query::clustering_range::bound> b2;
    if (!ck2.empty()) {
        b2.emplace(clustering_key_prefix::from_single_value(
                s, utf8_type->decompose(ck2)));
    }
    return partition_slice_builder(s).
            with_range(query::clustering_range(b1, b2)).build();
}

// Count the number of CQL rows in one partition between clustering key
// prefix ck1 to ck2.
static future<int> count_rows(test_env& env, sstable_ptr sstp, schema_ptr s, sstring key, sstring ck1, sstring ck2) {
    return seastar::async([&env, sstp, s, key, ck1, ck2] () mutable {
        auto ps = make_partition_slice(*s, ck1, ck2);
        auto pr = dht::partition_range::make_singular(make_dkey(s, key.c_str()));
        auto rd = sstp->make_reader(s, env.make_reader_permit(), pr, ps);
        auto close_rd = deferred_close(rd);
        auto mfopt = rd().get();
        if (!mfopt) {
            return 0;
        }
        int nrows = 0;
        mfopt = rd().get();
        while (mfopt) {
            if (mfopt->is_clustering_row()) {
                nrows++;
            }
            mfopt = rd().get();
        }
        return nrows;
    });
}

// Count the number of CQL rows in one partition
static future<int> count_rows(test_env& env, sstable_ptr sstp, schema_ptr s, sstring key) {
    return seastar::async([&env, sstp, s, key] () mutable {
        auto pr = dht::partition_range::make_singular(make_dkey(s, key.c_str()));
        auto rd = sstp->make_reader(s, env.make_reader_permit(), pr, s->full_slice());
        auto close_rd = deferred_close(rd);
        auto mfopt = rd().get();
        if (!mfopt) {
            return 0;
        }
        int nrows = 0;
        mfopt = rd().get();
        while (mfopt) {
            if (mfopt->is_clustering_row()) {
                nrows++;
            }
            mfopt = rd().get();
        }
        return nrows;
    });
}

// Count the number of CQL rows between clustering key prefix ck1 to ck2
// in all partitions in the sstable (using sstable::read_range_rows).
static future<int> count_rows(test_env& env, sstable_ptr sstp, schema_ptr s, sstring ck1, sstring ck2) {
    return seastar::async([&env, sstp, s, ck1, ck2] () mutable {
        auto ps = make_partition_slice(*s, ck1, ck2);
        auto reader = sstp->make_reader(s, env.make_reader_permit(), query::full_partition_range, ps);
        auto close_reader = deferred_close(reader);
        int nrows = 0;
        auto mfopt = reader().get();
        while (mfopt) {
            mfopt = reader().get();
            BOOST_REQUIRE(mfopt);
            while (!mfopt->is_end_of_partition()) {
                if (mfopt->is_clustering_row()) {
                    nrows++;
                }
                mfopt = reader().get();
            }
            mfopt = reader().get();
        }
        return nrows;
    });
}

// This test reads, using sstable::read_row(), a slice (a range of clustering
// rows) from one large partition in an sstable written in Cassandra.
// This large partition includes 13520 clustering rows, and spans about
// 700 KB on disk. When we ask to read only a part of it, the promoted index
// (included in this sstable) may be used to allow reading only a part of the
// partition from disk. This test doesn't directly verify that the promoted
// index is actually used - and can work even without a promoted index
// support - but can be used to check that adding promoted index read supports
// did not break anything.
// To verify that the promoted index was actually used to reduce the size
// of read from disk, add printouts to the row reading code.
SEASTAR_TEST_CASE(sub_partition_read) {
  schema_ptr s = large_partition_schema();
  return for_each_sstable_version([s] (const sstables::sstable::version_types version) {
    return test_env::do_with_async([s, version] (test_env& env) {
        auto sstp = load_large_partition_sst(env, version).get();
        {
            auto nrows = count_rows(env, sstp, s, "v1", "18wX", "18xB").get();
            // there should be 5 rows (out of 13520 = 20*26*26) in this range:
            // 18wX, 18wY, 18wZ, 18xA, 18xB.
            BOOST_REQUIRE(nrows == 5);
        }
        {
            auto nrows = count_rows(env, sstp, s, "v1", "13aB", "15aA").get();
            // There should be 26*26*2 rows in this range. It spans two
            // promoted-index blocks, so we get to test that case.
            BOOST_REQUIRE(nrows == 2*26*26);
        }
        {
            auto nrows = count_rows(env, sstp, s, "v1", "10aB", "19aA").get();
            // There should be 26*26*9 rows in this range. It spans many
            // promoted-index blocks.
            BOOST_REQUIRE(nrows == 9*26*26);
        }
        {
            auto nrows = count_rows(env, sstp, s, "v1", "0", "z").get();
            // All rows, 20*26*26 of them, are in this range. It spans all
            // the promoted-index blocks, but the range is still bounded
            // on both sides
            BOOST_REQUIRE(nrows == 20*26*26);
        }
        {
            // range that is outside (after) the actual range of the data.
            // No rows should match.
            auto nrows = count_rows(env, sstp, s, "v1", "y", "z").get();
            BOOST_REQUIRE(nrows == 0);
        }
        {
            // range that is outside (before) the actual range of the data.
            // No rows should match.
            auto nrows = count_rows(env, sstp, s, "v1", "_a", "_b").get();
            BOOST_REQUIRE(nrows == 0);
        }
        {
            // half-infinite range
            auto nrows = count_rows(env, sstp, s, "v1", "", "10aA").get();
            BOOST_REQUIRE(nrows == (1*26*26 + 1));
        }
        {
            // half-infinite range
            auto nrows = count_rows(env, sstp, s, "v1", "10aA", "").get();
            BOOST_REQUIRE(nrows == 19*26*26);
        }
        {
            // count all rows, but giving an explicit all-encompasing filter
            auto nrows = count_rows(env, sstp, s, "v1", "", "").get();
            BOOST_REQUIRE(nrows == 20*26*26);
        }
        {
            // count all rows, without a filter
            auto nrows = count_rows(env, sstp, s, "v1").get();
            BOOST_REQUIRE(nrows == 20*26*26);
        }
    });
  });
}

// Same as previous test, just using read_range_rows instead of read_row
// to read parts of potentially more than one partition (in this particular
// sstable, there is actually just one partition).
SEASTAR_TEST_CASE(sub_partitions_read) {
  schema_ptr s = large_partition_schema();
  return for_each_sstable_version([s] (const sstables::sstable::version_types version) {
   return test_env::do_with_async([s, version] (test_env& env) {
        auto sstp = load_large_partition_sst(env, version).get();
        auto nrows = count_rows(env, sstp, s, "18wX", "18xB").get();
        BOOST_REQUIRE(nrows == 5);
   });
  });
}

SEASTAR_TEST_CASE(test_skipping_in_compressed_stream) {
    return seastar::async([] {
        tests::reader_concurrency_semaphore_wrapper semaphore;

        tmpdir tmp;
        auto file_path = (tmp.path() / "test").string();
        file f = open_file_dma(file_path, open_flags::create | open_flags::wo).get();

        file_input_stream_options opts;
        opts.read_ahead = 0;

        compression_parameters cp({
            { compression_parameters::SSTABLE_COMPRESSION, "LZ4Compressor" },
            { compression_parameters::CHUNK_LENGTH_KB, std::to_string(opts.buffer_size/1024) },
        });

        sstables::compression c;
        // this initializes "c"
        auto os = make_file_output_stream(f, file_output_stream_options()).get();
        auto out = make_compressed_file_m_format_output_stream(std::move(os), &c, cp, make_lz4_sstable_compressor_for_tests());

        // Make sure that amount of written data is a multiple of chunk_len so that we hit #2143.
        temporary_buffer<char> buf1(c.uncompressed_chunk_length());
        strcpy(buf1.get_write(), "buf1");
        temporary_buffer<char> buf2(c.uncompressed_chunk_length());
        strcpy(buf2.get_write(), "buf2");

        size_t uncompressed_size = 0;
        out.write(buf1.get(), buf1.size()).get();
        uncompressed_size += buf1.size();
        out.write(buf2.get(), buf2.size()).get();
        uncompressed_size += buf2.size();
        out.close().get();

        auto compressed_size = seastar::file_size(file_path).get();
        c.update(compressed_size);

        lru compression_info_lru;
        logalloc::region compression_info_region;
        compression_info_cache_stats compression_info_stats;
        // The offsets are all in memory, in `c`, so the cache never touches the file.
        sstables::compression_info_cache compression_info(file(), c, compression_info_lru, compression_info_region, compression_info_stats);

        auto make_is = [&] {
            f = open_file_dma(file_path, open_flags::ro).get();
            auto stream_creator = [f](uint64_t pos, uint64_t len, file_input_stream_options options)->future<input_stream<char>> {
                co_return input_stream<char>(make_file_data_source(std::move(f), pos, len, std::move(options)));
            };
            return make_compressed_file_m_format_input_stream(stream_creator,
                    std::make_unique<sstables::compression_info_accessor>(compression_info),
                    0, uncompressed_size, opts, semaphore.make_permit(), std::nullopt);
        };

        auto expect = [] (input_stream<char>& in, const temporary_buffer<char>& buf) {
            auto b = in.read_exactly(buf.size()).get();
            BOOST_REQUIRE(b == buf);
        };

        auto expect_eof = [] (input_stream<char>& in) {
            auto b = in.read().get();
            BOOST_REQUIRE(b.empty());
        };

      {
        auto in = make_is();
        expect(in, buf1);
        expect(in, buf2);
        expect_eof(in);
      }

      {
        auto in = make_is();
        in.skip(0).get();
        expect(in, buf1);
        expect(in, buf2);
        expect_eof(in);
      }

      {
        auto in = make_is();
        expect(in, buf1);
        in.skip(0).get();
        expect(in, buf2);
        expect_eof(in);
      }

      {
        auto in = make_is();
        expect(in, buf1);
        in.skip(opts.buffer_size).get();
        expect_eof(in);
      }

      {
        auto in = make_is();
        in.skip(opts.buffer_size * 2).get();
        expect_eof(in);
      }

      {
        auto in = make_is();
        in.skip(opts.buffer_size).get();
        in.skip(opts.buffer_size).get();
        expect_eof(in);
      }
    });
}

// Test that sstables::key_view::tri_compare(const schema& s, partition_key_view other)
// should correctly compare empty keys. The fact we did this incorrectly was
// noticed while fixing #9375, and a separate issue on it is #10178.
BOOST_AUTO_TEST_CASE(test_empty_key_view_comparison) {
    auto s_ptr = schema_builder(1, "", "")
            .with_column("p", bytes_type, column_kind::partition_key)
            .build();
    const schema& s = *s_ptr;

    sstables::key empty_sstable_key = sstables::key::from_deeply_exploded(s, {data_value(bytes(""))});
    sstables::key_view empty_sstable_key_view = empty_sstable_key;
    partition_key empty_partition_key = partition_key::from_deeply_exploded(s, {data_value(bytes(""))});
    partition_key_view empty_partition_key_view = empty_partition_key;

    // Two empty keys should be equal (this check failed in #10178)
    BOOST_CHECK_EQUAL(std::strong_ordering::equal,
        empty_sstable_key_view.tri_compare(s, empty_partition_key_view));

    // For completeness, compare also an empty key to a non-empty key, and
    // two equal non-empty keys. An empty key is supposed to be less-than
    // a non-empty key.
    sstables::key hello_sstable_key = sstables::key::from_deeply_exploded(s, {data_value(bytes("hello"))});
    sstables::key_view hello_sstable_key_view = hello_sstable_key;
    partition_key hello_partition_key = partition_key::from_deeply_exploded(s, {data_value(bytes("hello"))});
    partition_key_view hello_partition_key_view = hello_partition_key;
    BOOST_CHECK_EQUAL(std::strong_ordering::less,
        empty_sstable_key_view.tri_compare(s, hello_partition_key_view));
    BOOST_CHECK_EQUAL(std::strong_ordering::greater,
        hello_sstable_key_view.tri_compare(s, empty_partition_key_view));
    BOOST_CHECK_EQUAL(std::strong_ordering::equal,
        hello_sstable_key_view.tri_compare(s, hello_partition_key_view));

    // The underlying cause of #10178 was that legacy_form() returned
    // a legacy_compound_view<> which, despite being empty, did not
    // have begin()==end(). So let's reproduce that directly:
    auto lf = empty_partition_key_view.legacy_form(s);
    BOOST_CHECK_EQUAL(0, lf.size());
    BOOST_CHECK(lf.begin() == lf.end());
}

static sstables::entry_descriptor make_entry_descriptor(sstables::generation_type gen, sstables::sstable_version_types version, sstables::sstable_format_types format, sstables::component_type component) {
    optimized_optional<sstables::sstable_id> sid;
    if (gen.is_uuid_based()) {
        sid = sstables::sstable_id(gen.as_uuid());
    }
    return entry_descriptor{
        gen,
        sid,
        version,
        format,
        component
    };
}

// Test that sstables::parse_path is able to parse the paths of sstables
BOOST_AUTO_TEST_CASE(test_parse_path_good) {
    struct sstable_case {
        std::string_view path;
        std::string_view ks;
        std::string_view cf;
        sstables::entry_descriptor desc;
    };
    const sstable_case sstables[] = {
        {
            "/scylla/system/truncated-38c19fd0fb863310a4b70d0cc66628aa/mc-2-big-Data.db",
            "system",
            "truncated",
            make_entry_descriptor(
                generation_type{2},
                sstable_version_types::mc,
                sstable_format_types::big,
                component_type::Data
            )
        },
        {
            "/scylla/system/scylla_local-2972ec7ffb2038ddaac1d876f2e3fcbd/mc-3-big-Summary.db",
            "system",
            "scylla_local",
            make_entry_descriptor(
                generation_type{3},
                sstable_version_types::mc,
                sstable_format_types::big,
                component_type::Summary
            )
        },
        {
            "/scylla/system_distributed/cdc_generation_timestamps-fdf455c4cfec3e009719d7a45436c89d/me-3g9p_0938_0ecz429c6f019i7yuf-big-Index.db",
            "system_distributed",
            "cdc_generation_timestamps",
            make_entry_descriptor(
                generation_type::from_string("3g9p_0938_0ecz429c6f019i7yuf"),
                sstable_version_types::me,
                sstable_format_types::big,
                component_type::Index
            )
         },
         {
            "/system_schema/columns-24101c25a2ae3af787c1b40ee1aca33f/md-3g9r_04ux_4be4w2d7t8bg6u7nok-big-Statistics.db",
            "system_schema",
            "columns",
            make_entry_descriptor(
                generation_type::from_string("3g9r_04ux_4be4w2d7t8bg6u7nok"),
                sstable_version_types::md,
                sstable_format_types::big,
                component_type::Statistics
            )
        },
        {
            "/system_schema/columns-24101c25a2ae3af787c1b40ee1aca33f/md-3g9r_04ux_4be4w2d7t8bg6u7nok-big-ReallyBigData.db",
            "system_schema",
            "columns",
            make_entry_descriptor(
                generation_type::from_string("3g9r_04ux_4be4w2d7t8bg6u7nok"),
                sstable_version_types::md,
                sstable_format_types::big,
                component_type::Unknown
            )
        }
    };
    for (auto& [path, expected_ks, expected_cf, expected_desc] : sstables) {
        auto [desc, ks, cf] = parse_path(path).value();
        BOOST_CHECK_EQUAL(ks, expected_ks);
        BOOST_CHECK_EQUAL(cf, expected_cf);
        BOOST_CHECK_EQUAL(expected_desc.generation, desc.generation);
        BOOST_CHECK_EQUAL(expected_desc.version, desc.version);
        BOOST_CHECK_EQUAL(expected_desc.format, desc.format);
        BOOST_CHECK_EQUAL(expected_desc.component, desc.component);
    }
}

// Test that sstables::parse_path throws at seeing malformed sstable names
BOOST_AUTO_TEST_CASE(test_parse_path_bad) {
    const std::string_view paths[] = {
        "",
        "/",
        "=",
        "hmm",
        "//-/-",
        "mc-2-big-Data.db",
        "truncated~38c19fd0fb863310a4b70d0cc66628aa/",
        "truncated-38c19fd0fb863310a4b70d0cc66628aa/mc-2-big-Data.db",
        "truncated~38c19fd0fb863310a4b70d0cc66628aa/mc-2-big-Data.db",
        "/scylla/system/truncated~38c19fd0fb863310a4b70d0cc66628aa/404.db",
        "/scylla/system/truncated~38c19fd0fb863310a4b70d0cc66628aa/mc/foo/bar.db",
        "/scylla/system/truncated~38c19fd0fb863310a4b70d0cc66628aa/mc/-------",
        "/scylla/system/truncated~38c19fd0fb863310a4b70d0cc66628aa/mc-2-big-Data.db",
        "/scylla/system/truncated-38c19fd0fb863310a4b70d0cc66628aa/zz-2-big-Data.db",
        "/scylla/system/truncated-38c19fd0fb863310a4b70d0cc66628aa/mc-x-big-Data.db",
        "/scylla/system/truncated-38c19fd0fb863310a4b70d0cc66628aa/mc-i~am~not~an~id-big-Data.db",
        "/scylla/system/truncated~38c19fd0fb863310a4b70d0cc66628aa/mc-2--Data.db",
        "/scylla/system/truncated~38c19fd0fb863310a4b70d0cc66628aa/mc-2-grand-Data.db",
    };
    for (auto path : paths) {
        BOOST_CHECK(!parse_path(path).has_value());
    }
}

using compress_sstable = tests::random_schema_specification::compress_sstable;
static future<> test_component_digest_persistence(component_type component, sstable::version_types version, compress_sstable compress = compress_sstable::no, bool rewrite_statistics = false) {
    return test_env::do_with_async([component, version, compress, rewrite_statistics] (test_env& env) mutable {
        auto random_spec = tests::make_random_schema_specification(
            "ks",
            std::uniform_int_distribution<size_t>(1, 4),
            std::uniform_int_distribution<size_t>(2, 4),
            std::uniform_int_distribution<size_t>(2, 8),
            std::uniform_int_distribution<size_t>(2, 8),
            compress);
        auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};
        auto schema = random_schema.schema();

        const auto muts = tests::generate_random_mutations(random_schema, 2).get();
        auto sst_original = make_sstable_containing(env.make_sstable(schema, version), muts).get();

        auto& components = sstables::test(sst_original).get_components();
        bool has_component = components.find(component) != components.end();
        BOOST_REQUIRE(has_component);

        auto toc_path = fmt::to_string(sst_original->toc_filename());
        auto entry_desc = sstables::parse_path(toc_path, schema->ks_name(), schema->cf_name()).value();
        auto dir_path = std::filesystem::path(toc_path).parent_path().string();

        std::optional<uint32_t> original_digest;
        if (rewrite_statistics) {
            auto original_sstable_id = sst_original->sstable_identifier();
            original_digest = sst_original->get_component_digest(component);
            BOOST_REQUIRE(original_digest.has_value());

            sst_original = mutate_sstable_level(env, sst_original, dir_path, 10, sstables::update_sstable_id::yes).get();
            entry_desc.generation = sst_original->generation();

            auto new_digest = sst_original->get_component_digest(component);
            BOOST_REQUIRE(new_digest.has_value());
            BOOST_REQUIRE(original_digest.value() != new_digest.value());

            BOOST_REQUIRE_NE(original_sstable_id, sst_original->sstable_identifier());
        }

        sst_original = nullptr;

        auto sst_reopened = env.make_sstable(schema, dir_path, entry_desc.generation, entry_desc.version, entry_desc.format);
        sst_reopened->load(schema->get_sharder()).get();

        auto loaded_digest = sst_reopened->get_component_digest(component);
        BOOST_REQUIRE(loaded_digest.has_value());

        auto f = open_file_dma(sstables::test(sst_reopened).filename(component).native(), open_flags::ro).get();
        auto stream = make_file_input_stream(f);
        auto close_stream = deferred_close(stream);
        auto component_data = util::read_entire_stream_contiguous(stream).get();
        auto calculated_digest = crc32_utils::checksum(component_data.begin(), component_data.size());
        BOOST_REQUIRE_EQUAL(calculated_digest, loaded_digest.value());

        // calculate scylla component digest, by reading file_size - sizeof(uint32_t)
        auto f2 = open_file_dma(sstables::test(sst_reopened).filename(sstables::component_type::Scylla).native(), open_flags::ro).get();
        auto stream2 = make_file_input_stream(f2);
        auto close_stream2 = deferred_close(stream2);
        auto scylla_data = util::read_entire_stream_contiguous(stream2).get();
        auto calc_scylla_digest = crc32_utils::checksum(scylla_data.begin(), scylla_data.size() - sizeof(uint32_t));
        BOOST_REQUIRE_EQUAL(calc_scylla_digest, sst_reopened->get_component_digest(sstables::component_type::Scylla).value());
    });
}

SEASTAR_TEST_CASE(test_digest_persistence_index) {
    return test_component_digest_persistence(component_type::Index, sstable::version_types::me);
}

SEASTAR_TEST_CASE(test_digest_persistence_partitions) {
    return test_component_digest_persistence(component_type::Partitions, sstable::version_types::ms);
}

SEASTAR_TEST_CASE(test_digest_persistence_rows) {
    return test_component_digest_persistence(component_type::Rows, sstable::version_types::ms);
}

SEASTAR_TEST_CASE(test_digest_persistence_summary) {
    return test_component_digest_persistence(component_type::Summary, sstable::version_types::me);
}

SEASTAR_TEST_CASE(test_digest_persistence_filter) {
    return test_component_digest_persistence(component_type::Filter, sstable::version_types::me);
}

SEASTAR_TEST_CASE(test_digest_persistence_compression) {
    return test_component_digest_persistence(component_type::CompressionInfo, sstable::version_types::me, compress_sstable::yes);
}

SEASTAR_TEST_CASE(test_digest_persistence_toc) {
    return test_component_digest_persistence(component_type::TOC, sstable::version_types::me);
}

SEASTAR_TEST_CASE(test_digest_persistence_statistics) {
    return test_component_digest_persistence(component_type::Statistics, sstable::version_types::me);
}

SEASTAR_TEST_CASE(test_digest_persistence_statistics_rewrite) {
    return test_component_digest_persistence(component_type::Statistics, sstable::version_types::me, compress_sstable::no, true);
}

SEASTAR_TEST_CASE(test_digest_persistence_data) {
    return test_component_digest_persistence(component_type::Data, sstable::version_types::me);
}

SEASTAR_TEST_CASE(test_digest_persistence_data_compressed) {
    return test_component_digest_persistence(component_type::Data, sstable::version_types::me, compress_sstable::yes);
}

static void corrupt_sstable(sstables::shared_sstable sst, component_type component) {
    auto path = sstables::test(sst).filename(component).native();
    auto size = seastar::file_size(path).get();
    auto f = open_file_dma(path, open_flags::rw).get();
    auto close_f = deferred_close(f);
    const auto mem_align = f.memory_dma_alignment();
    const auto dma_align = f.disk_write_dma_alignment();
    auto block_offset = align_down(size - 1, dma_align);
    auto buf = seastar::temporary_buffer<char>::aligned(mem_align, dma_align);
    f.dma_read(block_offset, buf.get_write(), dma_align).get();
    // Flip one bit in the last byte of the file to corrupt it minimally.
    // Using a single-bit flip avoids creating values that overflow
    // during parsing.
    buf.get_write()[size - 1 - block_offset] += 1;
    f.dma_write(block_offset, buf.get(), dma_align).get();
    f.truncate(size).get();
}

static future<> test_component_digest_validation(component_type component, sstable::version_types version, sstring expected_message, compress_sstable compress = compress_sstable::no) {
    return test_env::do_with_async([component, version, expected_message = std::move(expected_message), compress] (test_env& env) mutable {
        sstables::scoped_no_abort_on_malformed_sstable_error no_abort;
        auto random_spec = tests::make_random_schema_specification(
            "ks",
            std::uniform_int_distribution<size_t>(1, 4),
            std::uniform_int_distribution<size_t>(2, 4),
            std::uniform_int_distribution<size_t>(2, 8),
            std::uniform_int_distribution<size_t>(2, 8),
            compress);
        auto random_schema = tests::random_schema{tests::random::get_int<uint32_t>(), *random_spec};
        auto schema = random_schema.schema();

        const auto muts = tests::generate_random_mutations(random_schema, 2).get();
        auto sst = make_sstable_containing(env.make_sstable(schema, version), muts).get();

        auto digest = sst->get_component_digest(component);
        BOOST_REQUIRE(digest.has_value());

        auto toc_path = fmt::to_string(sst->toc_filename());
        auto entry_desc = sstables::parse_path(toc_path, schema->ks_name(), schema->cf_name()).value();
        auto dir_path = std::filesystem::path(toc_path).parent_path().string();

        corrupt_sstable(sst, component);

        BOOST_REQUIRE(sstables::validate_checksums_and_digests(sst, env.make_reader_permit()).get().status == validate_checksums_status::invalid);

        // Loading the sstable should detect the digest mismatch
        auto sst_corrupted = env.make_sstable(schema, dir_path, entry_desc.generation, entry_desc.version, entry_desc.format);
        BOOST_REQUIRE_EXCEPTION(sst_corrupted->load(schema->get_sharder()).get(), malformed_sstable_exception,
            exception_predicate::message_contains(expected_message));
    });
}

SEASTAR_TEST_CASE(test_digest_validation_statistics) {
    return test_component_digest_validation(component_type::Statistics, sstable::version_types::me, "Statistics digest mismatch");
}

SEASTAR_TEST_CASE(test_digest_validation_filter) {
    return test_component_digest_validation(component_type::Filter, sstable::version_types::me, "Filter digest mismatch");
}

SEASTAR_TEST_CASE(test_digest_validation_compression) {
    return test_component_digest_validation(component_type::CompressionInfo, sstable::version_types::me, "CompressionInfo digest mismatch", compress_sstable::yes);
}

SEASTAR_TEST_CASE(test_digest_validation_toc) {
    return test_component_digest_validation(component_type::TOC, sstable::version_types::me, "TOC digest mismatch");
}

SEASTAR_TEST_CASE(test_digest_validation_scylla) {
    return test_component_digest_validation(component_type::Scylla, sstable::version_types::me, "Scylla digest mismatch");
}

// Checks that the bit widths compression_info_bucket_layout picks for a chunk size
// are wide enough for every offset a bucket of chunks of the greatest length the
// layout allows (max_compressed_chunk_length()) can hold.
//
// Note that this only checks the layout against max_compressed_chunk_length(), not
// max_compressed_chunk_length() against the compressors; the bounds of those are
// documented next to it.
BOOST_AUTO_TEST_CASE(test_compression_info_bucket_layout_bit_widths) {
    auto test_chunk_size = [] (uint32_t chunk_size) {
        BOOST_TEST_CONTEXT("chunk_size=" << chunk_size) {
            const auto layout = sstables::compression_info_bucket_layout::for_chunk_size(chunk_size);
            const auto& packing = layout.packing;
            const uint64_t max_chunk_length = sstables::max_compressed_chunk_length(chunk_size);
            const uint32_t offsets_per_bucket = layout.offsets_per_bucket;
            const uint32_t grouped_offsets = packing.grouped_offsets();

            BOOST_REQUIRE_GE(grouped_offsets, 1u);
            BOOST_REQUIRE_LE(unsigned(packing.base_bits()), unsigned(sstables::max_field_bits));
            BOOST_REQUIRE_LE(unsigned(packing.relative_bits()), unsigned(sstables::max_field_bits));
            BOOST_REQUIRE_EQUAL(layout.segments_per_bucket,
                    (offsets_per_bucket + grouped_offsets - 1) / grouped_offsets);

            // The widest value a base offset field can be asked to hold is the
            // distance from the first offset of a bucket to the first offset of the
            // last segment of that bucket.
            const uint64_t max_base = uint64_t(((offsets_per_bucket - 1) / grouped_offsets) * grouped_offsets)
                    * max_chunk_length;
            BOOST_REQUIRE_EQUAL(max_base >> packing.base_bits(), 0u);
            // The widest value a relative offset field can be asked to hold is the
            // distance from the base offset of a segment to its last offset.
            if (grouped_offsets > 1) {
                const uint64_t max_relative = uint64_t(grouped_offsets - 1) * max_chunk_length;
                BOOST_REQUIRE_EQUAL(max_relative >> packing.relative_bits(), 0u);
            }

            // A whole bucket of maximally long chunks packs into storage_size bytes
            // and reads back exactly. Goes through the same functions
            // compression_info_cache::entry uses on the offsets it reads from the
            // file, so that the encoding under test is the one which ships.
            auto offset_of = [&] (uint32_t i) { return uint64_t(i) * max_chunk_length; };
            std::vector<char> raw_offsets(offsets_per_bucket * sizeof(uint64_t));
            for (uint32_t i = 0; i < offsets_per_bucket; ++i) {
                write_unaligned<uint64_t>(raw_offsets.data() + i * sizeof(uint64_t), net::hton(offset_of(i)));
            }

            constexpr size_t guard_size = 64;
            std::vector<char> storage(layout.storage_size + guard_size, 0);
            const uint64_t base = sstables::pack_bucket_offsets(layout, storage.data(), raw_offsets.data(),
                    offsets_per_bucket, 0);
            BOOST_REQUIRE_EQUAL(base, offset_of(0));
            // Nothing was written past the storage the layout asks for, including
            // through the unaligned 64-bit words the fields are written with.
            BOOST_REQUIRE(std::all_of(storage.begin() + layout.storage_size, storage.end(),
                    [] (char c) { return c == 0; }));
            for (uint32_t i = 0; i < offsets_per_bucket; ++i) {
                BOOST_REQUIRE_EQUAL(sstables::unpack_bucket_offset(layout, storage.data(), base, i), offset_of(i));
            }
        }
    };

    // Every chunk size which can be configured (a power of two, capped at 128 kiB by
    // compression_parameters::validate()), ...
    for (uint32_t chunk_size = 1024; chunk_size <= 128 * 1024; chunk_size *= 2) {
        test_chunk_size(chunk_size);
    }
    // ... and sizes which can only come from a foreign or corrupt CompressionInfo.db.
    for (uint32_t chunk_size : {1u, 2u, 3u, 100u, 4095u, 4097u, 1u << 20, 1u << 31,
                                std::numeric_limits<uint32_t>::max()}) {
        test_chunk_size(chunk_size);
    }
}

// Writes a synthetic CompressionInfo.db with `chunk_count` chunk offsets, and checks
// that compression_info_cache serves all of them by reading them back from the file,
// without any help from the in-memory copy in sstables::compression.
SEASTAR_TEST_CASE(test_compression_info_cache_reads_offsets_from_file) {
    return seastar::async([] {
        constexpr uint32_t chunk_length = 4096;

        tmpdir tmp;
        auto path = (tmp.path() / "CompressionInfo.db").string();

        sstables::compression c;
        c.set_compressor(make_lz4_sstable_compressor_for_tests());
        c.set_uncompressed_chunk_length(chunk_length);

        const auto layout = sstables::compression_info_bucket_layout::for_chunk_size(chunk_length);
        // Enough chunks for a few full buckets plus a partial last one.
        const uint64_t chunk_count = 3 * layout.offsets_per_bucket + 17;

        // Chunk lengths are arbitrary, they only have to stay within the bound the
        // packing of the offsets assumes.
        std::vector<uint64_t> offsets;
        auto writer = c.offsets.get_writer();
        uint64_t pos = 0;
        for (uint64_t i = 0; i < chunk_count; ++i) {
            offsets.push_back(pos);
            writer.push_back(pos);
            pos += tests::random::get_int<uint32_t>(1, chunk_length + 64);
        }
        c.set_uncompressed_file_length(chunk_count * chunk_length);
        c.set_compressed_file_length(pos);

        {
            auto f = open_file_dma(path, open_flags::create | open_flags::wo).get();
            auto fw = sstables::file_writer(make_file_output_stream(std::move(f)).get());
            sstables::write(sstable_version_types::me, fw, c);
            fw.close();
        }

        // The offsets are the last thing in the file, so this pins down the position
        // write() recorded.
        BOOST_REQUIRE_EQUAL(c.chunk_count(), chunk_count);
        BOOST_REQUIRE_EQUAL(c.offsets_start_pos() + chunk_count * sizeof(uint64_t),
                seastar::file_size(path).get());

        // A compression without the in-memory copy of the offsets, so that a lookup
        // which doesn't go to the file can't accidentally return the right answer.
        sstables::compression c2;
        c2.set_compressor(make_lz4_sstable_compressor_for_tests());
        c2.set_uncompressed_chunk_length(chunk_length);
        c2.set_uncompressed_file_length(c.uncompressed_file_length());
        c2.set_compressed_file_length(c.compressed_file_length());
        c2.set_offsets_start_pos(c.offsets_start_pos());
        c2.set_chunk_count(chunk_count);
        c2.set_offsets_evictable(true);

        auto f = open_file_dma(path, open_flags::ro).get();
        auto close_f = deferred_close(f);

        lru cache_lru;
        logalloc::region region;
        compression_info_cache_stats stats;
        {
            sstables::compression_info_cache cache(f, c2, cache_lru, region, stats);
            BOOST_REQUIRE(cache.paged());

            const uint64_t bucket_count = (chunk_count + layout.offsets_per_bucket - 1) / layout.offsets_per_bucket;

            auto verify = [&] (std::vector<uint64_t> order, use_caching caching) {
                sstables::compression_info_accessor acc(cache, caching);
                BOOST_REQUIRE_EQUAL(acc.chunk_count(), chunk_count);
                for (uint64_t i : order) {
                    const uint64_t expected_start = offsets[i];
                    const uint64_t expected_end = (i + 1 < chunk_count) ? offsets[i + 1] : c.compressed_file_length();

                    auto chunk = acc.get_chunk_by_index(i).get();
                    BOOST_REQUIRE_EQUAL(chunk.chunk_start, expected_start);
                    BOOST_REQUIRE_EQUAL(chunk.chunk_len, expected_end - expected_start);
                    BOOST_REQUIRE_EQUAL(chunk.offset, 0u);

                    const unsigned offset_in_chunk = (i * 7) % chunk_length;
                    auto located = acc.locate(i * chunk_length + offset_in_chunk).get();
                    BOOST_REQUIRE_EQUAL(located.chunk_start, expected_start);
                    BOOST_REQUIRE_EQUAL(located.chunk_len, expected_end - expected_start);
                    BOOST_REQUIRE_EQUAL(located.offset, offset_in_chunk);
                }
            };

            std::vector<uint64_t> forward;
            for (uint64_t i = 0; i < chunk_count; ++i) {
                forward.push_back(i);
            }

            // A sequential scan reads every bucket exactly once.
            verify(forward, use_caching::yes);
            BOOST_REQUIRE_EQUAL(stats.misses, bucket_count);
            BOOST_REQUIRE_EQUAL(stats.populations, bucket_count);
            BOOST_REQUIRE_EQUAL(stats.evictions, 0u);
            BOOST_REQUIRE_GT(stats.used_bytes, 0u);
            // Every bucket is accounted for with the same size.
            BOOST_REQUIRE_EQUAL(stats.used_bytes % bucket_count, 0u);

            // Now every bucket is cached, so a second pass (in a random order, and
            // backwards) doesn't read anything.
            auto backward = forward;
            std::ranges::reverse(backward);
            auto shuffled = forward;
            std::shuffle(shuffled.begin(), shuffled.end(), tests::random::gen());
            const auto misses_after_first_pass = stats.misses;
            verify(backward, use_caching::yes);
            verify(shuffled, use_caching::yes);
            BOOST_REQUIRE_EQUAL(stats.misses, misses_after_first_pass);

            // Evicted buckets are read again, and the accounting is balanced.
            cache_lru.evict_all();
            BOOST_REQUIRE_EQUAL(stats.evictions, bucket_count);
            BOOST_REQUIRE_EQUAL(stats.used_bytes, 0u);
            verify(forward, use_caching::yes);
            BOOST_REQUIRE_EQUAL(stats.misses, 2 * bucket_count);

            // A BYPASS CACHE read gets private buckets: it neither hits nor
            // populates the shared cache.
            const auto stats_before_bypass = stats;
            cache_lru.evict_all();
            verify(forward, use_caching::no);
            BOOST_REQUIRE_EQUAL(stats.hits, stats_before_bypass.hits);
            BOOST_REQUIRE_EQUAL(stats.populations, stats_before_bypass.populations);
            BOOST_REQUIRE_EQUAL(stats.used_bytes, 0u);

            {
                sstables::compression_info_accessor acc(cache, use_caching::yes);
                BOOST_REQUIRE_THROW(acc.get_chunk_by_index(chunk_count).get(), std::out_of_range);
            }
        }
        BOOST_REQUIRE_EQUAL(stats.used_bytes, 0u);
    });
}

// Reads the chunk offsets of the given sstable straight out of its CompressionInfo.db,
// bypassing sstables::compression and compression_info_cache. The reference the tests
// of the cache check themselves against.
static std::vector<uint64_t> read_chunk_offsets_from_file(const shared_sstable& sst) {
    const auto& c = sst->get_compression();
    auto f = open_file_dma(sst->get_filename(component_type::CompressionInfo).format(), open_flags::ro).get();
    auto close_f = deferred_close(f);
    auto buf = f.dma_read_exactly<char>(c.offsets_start_pos(), c.chunk_count() * sizeof(uint64_t)).get();
    BOOST_REQUIRE_EQUAL(buf.size(), c.chunk_count() * sizeof(uint64_t));
    std::vector<uint64_t> offsets;
    for (uint64_t i = 0; i < c.chunk_count(); ++i) {
        offsets.push_back(net::ntoh(read_unaligned<uint64_t>(buf.get() + i * sizeof(uint64_t))));
    }
    return offsets;
}

// Checks that a compressed sstable loaded from disk serves the chunk offsets which are
// in its CompressionInfo.db, both when it serves them out of the evictable cache and
// when compressioninfo_is_evictable is off and they are served from memory.
static future<> test_compression_info_cache_of_loaded_sstable(bool evictable) {
    return test_env::do_with_async([evictable] (test_env& env) {
        schema_builder builder(1, "ks", "cf");
        builder.with_column("pk", utf8_type, column_kind::partition_key);
        builder.with_column("ck", utf8_type, column_kind::clustering_key);
        builder.with_column("v", utf8_type);
        builder.set_compressor_params(compression_parameters({
            {compression_parameters::SSTABLE_COMPRESSION, "LZ4Compressor"},
            {compression_parameters::CHUNK_LENGTH_KB, "1"},
        }));
        auto s = builder.build();

        // Enough data to span several chunks.
        utils::chunked_vector<mutation> muts;
        mutation m(s, partition_key::from_exploded(*s, {to_bytes("pk")}));
        for (int i = 0; i < 512; ++i) {
            auto ck = clustering_key::from_exploded(*s, {to_bytes(format("ck{:04d}", i))});
            m.set_clustered_cell(ck, "v", data_value(tests::random::get_sstring(256)), api::new_timestamp());
        }
        muts.push_back(std::move(m));

        auto written = make_sstable_containing(env.make_sstable(s), muts).get();
        auto sst = env.reusable_sst(s, written).get();

        auto& cache = sst->get_compression_info_cache();
        const auto& c = sst->get_compression();
        BOOST_REQUIRE_EQUAL(cache.paged(), evictable);
        BOOST_REQUIRE_GT(c.chunk_count(), 1u);
        // With the evictable cache, the offsets aren't parsed into memory at all.
        BOOST_REQUIRE_EQUAL(c.offsets.size(), evictable ? 0 : c.chunk_count());

        auto reference = read_chunk_offsets_from_file(sst);
        {
            sstables::compression_info_accessor acc(cache);
            for (uint64_t i = 0; i < c.chunk_count(); ++i) {
                const uint64_t expected_start = reference[i];
                const uint64_t expected_end = (i + 1 < c.chunk_count())
                        ? reference[i + 1]
                        : c.compressed_file_length();
                auto chunk = acc.get_chunk_by_index(i).get();
                BOOST_REQUIRE_EQUAL(chunk.chunk_start, expected_start);
                BOOST_REQUIRE_EQUAL(chunk.chunk_len, expected_end - expected_start);
            }
        }

        // The data reads back correctly through the same offsets.
        assert_that(sst->as_mutation_source().make_mutation_reader(s, env.make_reader_permit()))
                .produces(muts[0])
                .produces_end_of_stream();

        if (evictable) {
            auto& tracker = env.manager().get_cache_tracker();
            auto& stats = tracker.get_compression_info_cache_stats();
            tracker.get_compression_info_lru().evict_all();
            BOOST_REQUIRE_EQUAL(stats.used_bytes, 0u);
            const auto stats_before_bypass = stats;

            auto reversed_schema = s->make_reversed();
            auto slice = partition_slice_builder(*reversed_schema, reversed_schema->full_slice())
                    .with_option<query::partition_slice::option::reversed>()
                    .with_option<query::partition_slice::option::bypass_cache>()
                    .build();
            auto range = dht::partition_range::make_singular(muts[0].decorated_key());
            auto rd = sst->make_reader(reversed_schema, env.make_reader_permit(), range, slice);
            auto close_rd = deferred_close(rd);
            size_t fragments = 0;
            while (rd().get()) {
                ++fragments;
            }
            BOOST_REQUIRE_GT(fragments, 0u);

            // Reversed BYPASS CACHE reads use private compression-info buckets.
            BOOST_REQUIRE_EQUAL(stats.hits, stats_before_bypass.hits);
            BOOST_REQUIRE_EQUAL(stats.populations, stats_before_bypass.populations);
            BOOST_REQUIRE_EQUAL(stats.used_bytes, 0u);
        }
    }, {.compressioninfo_is_evictable = evictable});
}

SEASTAR_TEST_CASE(test_compression_info_cache_of_loaded_sstable_evictable) {
    return test_compression_info_cache_of_loaded_sstable(true);
}

SEASTAR_TEST_CASE(test_compression_info_cache_of_loaded_sstable_not_evictable) {
    return test_compression_info_cache_of_loaded_sstable(false);
}
