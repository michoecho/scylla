/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

// Tests for sstable_datafile_cursor (see sstables/compressed_file_cursor.cc),
// which reads an sstable data file by logical (uncompressed) position, both
// forwards and backwards, transparently handling the compressed and
// uncompressed cases.
//
// The cursor's job is to return exactly the bytes that sstable::data_read()
// would return for the same logical range, so most tests here use data_read()
// as the ground truth and compare the cursor's output against it. Each test is
// run against both an uncompressed and an lz4-compressed sstable so that both
// cursor implementations are exercised.

#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/closeable.hh>

#include "sstables/sstables.hh"
#include "sstables/compressed_file_cursor.hh"
#include "sstables/sstable_datafile_position.hh"
#include "schema/schema_builder.hh"
#include "test/boost/sstable_test.hh"
#include "test/lib/sstable_utils.hh"
#include "test/lib/make_random_string.hh"
#include "test/lib/random_utils.hh"

using namespace sstables;

namespace {

// Builds a schema with a single text clustering column and a single text
// regular column. `compress` controls whether the data file is compressed; the
// compression chunk length is deliberately small so even modest data files
// span several chunks.
schema_ptr make_test_schema(bool compress) {
    auto builder = schema_builder(this_smp_shard_count(), "ks", "cursor_test")
        .with_column("pk", int32_type, column_kind::partition_key)
        .with_column("ck", utf8_type, column_kind::clustering_key)
        .with_column("v", utf8_type);
    if (compress) {
        builder.set_compressor_params(compression_parameters({
            {compression_parameters::SSTABLE_COMPRESSION, "LZ4Compressor"},
            {compression_parameters::CHUNK_LENGTH_KB, "4"},
        }));
    } else {
        builder.set_compressor_params(compression_parameters::no_compression());
    }
    return builder.build();
}

// Creates an sstable with `partitions` partitions, each holding `rows_per_partition`
// rows whose values are `value_size` bytes long. Picking these so that the
// resulting data file comfortably spans multiple compression chunks and disk
// blocks lets the tests cover cross-chunk and cross-block reads.
shared_sstable make_populated_sstable(test_env& env, schema_ptr s, int partitions, int rows_per_partition, size_t value_size) {
    utils::chunked_vector<mutation> muts;
    for (int p = 0; p < partitions; ++p) {
        auto pk = partition_key::from_single_value(*s, int32_type->decompose(p));
        mutation m(s, pk);
        for (int r = 0; r < rows_per_partition; ++r) {
            auto ck = clustering_key::from_single_value(*s, utf8_type->decompose(seastar::format("ck{:08d}", r)));
            auto& cell = m.partition().clustered_row(*s, ck).cells();
            auto cdef = s->get_column_definition("v");
            cell.apply(*cdef, atomic_cell::make_live(*utf8_type, api::new_timestamp(),
                    utf8_type->decompose(make_random_string(value_size))));
        }
        muts.push_back(std::move(m));
    }
    return make_sstable_containing(env.make_sstable(s), std::move(muts)).get();
}

// The ground-truth contents of the sstable's data file: the decompressed bytes
// as returned by the established data_read() path.
bytes read_whole_data_file(shared_sstable sst, reader_permit permit) {
    uint64_t size = sst->data_size();
    auto buf = sst->data_read(0, size, permit).get();
    BOOST_REQUIRE_EQUAL(buf.size(), size);
    return bytes(reinterpret_cast<const int8_t*>(buf.get()), buf.size());
}

bytes_view sub(const bytes& b, uint64_t start, uint64_t end) {
    return bytes_view(b).substr(start, end - start);
}

// Reads `n` bytes forwards from the cursor at logical position `pos` and checks
// they match the expected file contents.
void check_read_forwards(sstable_datafile_cursor& cur, const bytes& expected, uint64_t pos, size_t n) {
    cur.seek(sstable_datafile_position::from_logical_fixme(pos));
    auto buf = cur.read_forwards(n).get();
    uint64_t expected_end = std::min<uint64_t>(pos + n, expected.size());
    auto want = sub(expected, pos, expected_end);
    BOOST_REQUIRE_EQUAL(bytes_view(reinterpret_cast<const int8_t*>(buf.get()), buf.size()), want);
}

// Reads the logical range [start, end) backwards from the cursor and checks the
// bytes match the expected file contents.
void check_read(sstable_datafile_cursor& cur, const bytes& expected, uint64_t start, uint64_t end) {
    auto buf = cur.read(sstable_datafile_position::from_logical_fixme(start),
                        sstable_datafile_position::from_logical_fixme(end)).get();
    auto want = sub(expected, start, end);
    BOOST_REQUIRE_EQUAL(bytes_view(reinterpret_cast<const int8_t*>(buf.get()), buf.size()), want);
}

void run_for_each_sstable(test_env& env, std::function<void(shared_sstable, const bytes&, reader_permit)> test) {
    for (bool compress : {false, true}) {
        BOOST_TEST_MESSAGE(seastar::format("compress={}", compress));
        auto s = make_test_schema(compress);
        auto sst = make_populated_sstable(env, s, 4, 64, 200);
        auto permit = env.make_reader_permit();
        auto expected = read_whole_data_file(sst, permit);
        // Sanity: the data file must span more than one 4KB chunk so the tests
        // are actually exercising cross-chunk behavior.
        BOOST_REQUIRE_GT(expected.size(), 16 * 1024u);
        test(sst, expected, permit);
    }
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(test_cursor_read_forwards_whole_file) {
    test_env::do_with_async([] (test_env& env) {
        run_for_each_sstable(env, [] (shared_sstable sst, const bytes& expected, reader_permit permit) {
            sstable_datafile_cursor cur(sst, permit, {});
            auto close = deferred_close(cur);
            // Read the whole file forwards in small, unaligned chunks and
            // accumulate; the result must equal the whole file.
            cur.seek(sstable_datafile_position::from_logical_fixme(0));
            bytes got;
            while (true) {
                auto buf = cur.read_forwards(333).get();
                if (buf.empty()) {
                    break;
                }
                got.append(reinterpret_cast<const int8_t*>(buf.get()), buf.size());
            }
            BOOST_REQUIRE_EQUAL(got, expected);
        });
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_cursor_read_backwards_whole_file) {
    test_env::do_with_async([] (test_env& env) {
        run_for_each_sstable(env, [] (shared_sstable sst, const bytes& expected, reader_permit permit) {
            sstable_datafile_cursor cur(sst, permit, {});
            auto close = deferred_close(cur);
            // Read the whole file backwards in small, unaligned chunks; prepend
            // each chunk so the accumulated result is in forward order.
            uint64_t end = expected.size();
            bytes got;
            while (end > 0) {
                uint64_t start = end >= 333 ? end - 333 : 0;
                auto buf = cur.read(sstable_datafile_position::from_logical_fixme(start),
                                    sstable_datafile_position::from_logical_fixme(end)).get();
                bytes chunk(reinterpret_cast<const int8_t*>(buf.get()), buf.size());
                got = chunk + got;
                end = start;
            }
            BOOST_REQUIRE_EQUAL(got, expected);
        });
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_cursor_random_reads) {
    test_env::do_with_async([] (test_env& env) {
        run_for_each_sstable(env, [] (shared_sstable sst, const bytes& expected, reader_permit permit) {
            sstable_datafile_cursor cur(sst, permit, {});
            auto close = deferred_close(cur);
            uint64_t size = expected.size();
            // A fresh cursor per direction would hide cache-reuse bugs; reuse
            // one cursor across many randomly-placed reads instead.
            for (int i = 0; i < 200; ++i) {
                if (tests::random::get_bool()) {
                    uint64_t pos = tests::random::get_int<uint64_t>(0, size);
                    size_t n = tests::random::get_int<size_t>(1, 9000);
                    check_read_forwards(cur, expected, pos, n);
                } else {
                    // A backwards read of the range [start, end).
                    uint64_t end = tests::random::get_int<uint64_t>(0, size);
                    size_t n = tests::random::get_int<size_t>(1, 9000);
                    uint64_t start = end >= n ? end - n : 0;
                    check_read(cur, expected, start, end);
                }
            }
        });
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_cursor_reads_spanning_chunk_boundaries) {
    test_env::do_with_async([] (test_env& env) {
        run_for_each_sstable(env, [] (shared_sstable sst, const bytes& expected, reader_permit permit) {
            sstable_datafile_cursor cur(sst, permit, {});
            auto close = deferred_close(cur);
            // The compressed chunk length is 4KB; deliberately straddle each
            // chunk boundary both forwards and backwards.
            for (uint64_t boundary = 4096; boundary < expected.size(); boundary += 4096) {
                check_read_forwards(cur, expected, boundary - 100, 200);
                check_read(cur, expected, boundary - 100, boundary + 100);
            }
        });
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_cursor_read_at_and_past_eof) {
    test_env::do_with_async([] (test_env& env) {
        run_for_each_sstable(env, [] (shared_sstable sst, const bytes& expected, reader_permit permit) {
            sstable_datafile_cursor cur(sst, permit, {});
            auto close = deferred_close(cur);
            uint64_t size = expected.size();
            // Reading at EOF returns nothing.
            cur.seek(sstable_datafile_position::from_logical_fixme(size));
            BOOST_REQUIRE(cur.read_forwards(1000).get().empty());
            // A read straddling EOF is truncated to the available bytes.
            check_read_forwards(cur, expected, size - 50, 1000);
            // A backwards read of the whole file ending at EOF.
            check_read(cur, expected, 0, size);
        });
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_cursor_compute_relative_position) {
    test_env::do_with_async([] (test_env& env) {
        run_for_each_sstable(env, [] (shared_sstable sst, const bytes& expected, reader_permit permit) {
            sstable_datafile_cursor cur(sst, permit, {});
            auto close = deferred_close(cur);
            uint64_t size = expected.size();
            uint64_t base = size / 2;
            cur.seek(sstable_datafile_position::from_logical_fixme(base));
            // compute_relative_position is relative to the seeked position and
            // does not consume input.
            BOOST_REQUIRE(cur.compute_relative_position(0) == sstable_datafile_position::from_logical_fixme(base));
            BOOST_REQUIRE(cur.compute_relative_position(100) == sstable_datafile_position::from_logical_fixme(base + 100));
            BOOST_REQUIRE(cur.compute_relative_position(-100) == sstable_datafile_position::from_logical_fixme(base - 100));
            // The position computed by stepping back can be used to read the
            // preceding range, matching the file contents there.
            auto prev = cur.compute_relative_position(-100);
            cur.seek(prev);
            check_read_forwards(cur, expected, base - 100, 100);
        });
    }).get();
}

// Reads the whole file forwards in order through `cur`, in small chunks.
void read_whole_file_forwards(sstable_datafile_cursor& cur, uint64_t size) {
    cur.seek(sstable_datafile_position::from_logical_fixme(0));
    uint64_t got = 0;
    while (got < size) {
        auto buf = cur.read_forwards(333).get();
        if (buf.empty()) {
            break;
        }
        got += buf.size();
    }
}

// The compressed cursor verifies the whole-file digest when it is read in order
// from the start. A correct digest passes; a corrupted one is detected; and a
// non-sequential read silently disables the check (so it never false-positives).
SEASTAR_THREAD_TEST_CASE(test_cursor_whole_file_digest) {
    test_env::do_with_async([] (test_env& env) {
        auto s = make_test_schema(true);
        auto sst = make_populated_sstable(env, s, 4, 64, 200);
        auto permit = env.make_reader_permit();
        auto expected = read_whole_data_file(sst, permit);
        BOOST_REQUIRE_GT(expected.size(), 16 * 1024u);

        auto digest = sst->read_digest().get();
        BOOST_REQUIRE(digest.has_value());

        // A correct digest, read in order from the start, verifies fine.
        {
            sstable_datafile_cursor cur(sst, permit, {}, digest);
            auto close = deferred_close(cur);
            read_whole_file_forwards(cur, expected.size());
        }

        // A corrupted digest is detected once the last chunk is folded in.
        {
            sstable_datafile_cursor cur(sst, permit, {}, *digest + 1);
            auto close = deferred_close(cur);
            BOOST_REQUIRE_EXCEPTION(read_whole_file_forwards(cur, expected.size()),
                    malformed_sstable_exception, [] (const malformed_sstable_exception& e) {
                        return sstring(e.what()).find("Digest mismatch") != sstring::npos;
                    });
        }

        // A non-sequential read leaves digest-calculating mode, so even a wrong
        // digest must not trigger a mismatch: read backwards (last chunk first).
        {
            sstable_datafile_cursor cur(sst, permit, {}, *digest + 1);
            auto close = deferred_close(cur);
            uint64_t end = expected.size();
            while (end > 0) {
                uint64_t start = end >= 333 ? end - 333 : 0;
                cur.read(sstable_datafile_position::from_logical_fixme(start),
                         sstable_datafile_position::from_logical_fixme(end)).get();
                end = start;
            }
        }
    }).get();
}

SEASTAR_THREAD_TEST_CASE(test_cursor_drop_caches_preserves_correctness) {
    test_env::do_with_async([] (test_env& env) {
        run_for_each_sstable(env, [] (shared_sstable sst, const bytes& expected, reader_permit permit) {
            sstable_datafile_cursor cur(sst, permit, {});
            auto close = deferred_close(cur);
            uint64_t size = expected.size();
            uint64_t mid = size / 2;
            // Warm the cache by reading across the whole file.
            check_read_forwards(cur, expected, 0, size);
            // Dropping caches must not change the bytes returned afterwards:
            // the cursor just refetches from disk.
            cur.drop_caches_after(sstable_datafile_position::from_logical_fixme(mid));
            check_read_forwards(cur, expected, mid, size - mid);
            check_read_forwards(cur, expected, 0, mid);
            cur.drop_caches_before(sstable_datafile_position::from_logical_fixme(mid));
            check_read(cur, expected, 0, mid);
            check_read_forwards(cur, expected, mid, size - mid);
        });
    }).get();
}
