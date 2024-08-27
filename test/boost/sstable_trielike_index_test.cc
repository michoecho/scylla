// #pragma clang optimize off

#include "readers/from_mutations_v2.hh"
#include "schema/schema_builder.hh"
#include "seastar/testing/test_case.hh"
#include "test/lib/mutation_reader_assertions.hh"
#include "test/lib/sstable_test_env.hh"
#include "sstables/sstable_writer.hh"

std::vector<dht::decorated_key> generate_interesting_keys(schema_ptr s, size_t group_size, size_t groups) {
    std::map<std::byte, std::map<std::byte, dht::decorated_key>> map;
    size_t done = 0;
    for (int64_t i = 0; done < groups; ++i) {
        auto pk = partition_key::from_exploded(*s, { long_type->decompose(i) });
        auto dk = dht::decorate_key(*s, pk);
        auto token = std::bit_cast<std::array<std::byte, 8>>(seastar::cpu_to_be(dk.token().unbias()));
        auto [it, inserted] = map.insert({token[0], {}});
        if (it->second.size() < group_size) {
            it->second.emplace(token[1], dk);
            if (it->second.size() == group_size) {
                done += 1;
            }
        }
    }
    std::vector<dht::decorated_key> results;
    for (const auto& [b0, cont] : map) {
        if (cont.size() != group_size) {
            continue;
        }
        for (const auto& [b1, dk] : cont) {
            results.push_back(dk);
        }
    }
    assert(results.size() == group_size * groups);
    assert(std::ranges::is_sorted(results, {}, [] (const auto& dk) {
        return std::bit_cast<std::array<std::byte, 8>>(seastar::cpu_to_be(dk.token().unbias()));
    }));
    return results;
}

SEASTAR_TEST_CASE(test_exhaustive) {
    return smp::invoke_on_all([] {
    return sstables::test_env::do_with_async([] (sstables::test_env& env) {
        auto s = schema_builder("ks", "cf")
            .with_column("key", bytes_type, column_kind::partition_key)
            .build();
        auto version = sstables::sstable_version_types::me;
        auto sst = env.make_sstable(s, version);
        auto cfg = env.manager().configure_writer();
        auto wr = sst->get_writer(*s, 1, cfg, encoding_stats{});
        std::vector<std::pair<dht::decorated_key, uint64_t>> index_entries;
        wr.attach_index_callback([&] (const dht::decorated_key& dk, uint64_t off)  {
            index_entries.push_back({dk, off});
        });
        auto permit = env.make_reader_permit();
        const size_t group_size = 3;
        auto interesting_keys = generate_interesting_keys(s, group_size, 2);
        auto muts = std::vector<mutation>();
        muts.push_back(mutation(s, interesting_keys[group_size/2]));
        muts.push_back(mutation(s, interesting_keys[group_size/2 + group_size]));
        auto mut_reader = make_mutation_reader_from_mutations_v2(s, permit, muts);
        mut_reader.consume_in_thread(std::move(wr));
        auto close_mut_reader = deferred_close(mut_reader);
        sst->load(sst->get_schema()->get_sharder()).get();
        std::vector<dht::partition_range> valid_ranges;
        auto gen_ringpos = [] (dht::decorated_key dk) {
            return std::vector<dht::ring_position>{
                {dk.token(), dht::ring_position::token_bound::start},
                {dk.token(), dk.key()},
                {dk.token(), dht::ring_position::token_bound::end},
            };
        };
        auto gen_bounds = [] (dht::decorated_key dk) {
            return std::vector<std::optional<dht::partition_range::bound>>{
                {},
                {{{dk.token(), dht::ring_position::token_bound::start}, true}},
                {{{dk.token(), dk.key()}, true}},
                {{{dk.token(), dht::ring_position::token_bound::end}, true}},
                {{{dk.token(), dht::ring_position::token_bound::start}, false}},
                {{{dk.token(), dk.key()}, false}},
                {{{dk.token(), dht::ring_position::token_bound::end}, false}},
            };
        };
        for (size_t i = 0; i < interesting_keys.size(); ++i) {
            for (size_t j = 0; j < interesting_keys.size(); ++j) {
                for (const auto& lb : gen_bounds(interesting_keys[i])) {
                    for (const auto& rb : gen_bounds(interesting_keys[j])) {
                        auto r = wrapping_interval<dht::ring_position>(lb, rb);
                        if (!r.is_wrap_around(dht::ring_position_comparator(*s))) {
                            valid_ranges.push_back(dht::partition_range(r));
                        }
                    }
                }
            }
            for (const auto& rp : gen_ringpos(interesting_keys[i])) {
                valid_ranges.push_back(dht::partition_range::make_singular(rp));
            }
        }
        auto intersecting_keys = [] (schema_ptr s, const dht::partition_range& pr, std::span<const mutation> ms) {
            std::vector<dht::decorated_key> result;
            for (const auto& m : ms) {
                if (pr.contains(m.decorated_key(), dht::ring_position_comparator(*s))) {
                    result.push_back(m.decorated_key());
                }
            }
            return result;
        };
        fmt::println("Valid ranges: {}", valid_ranges.size());
        size_t tries = 0;
        for (bool forwarding : {true, false})
        for (const auto& first_range : valid_ranges)
        for (const auto& second_range : valid_ranges) {
            tries += 1;
            if (tries % 100000 == 0) {
                fmt::println("Shard {}: {}", this_shard_id(), tries);
            }
            if (XXH64(&tries, sizeof(tries), 0) % smp::count != this_shard_id()) {
                continue;
            }
            auto r = sst->make_reader(s, permit, first_range, s->full_slice(), {}, streamed_mutation::forwarding::no, mutation_reader::forwarding(forwarding), sstables::default_read_monitor(), nullptr);
            // auto close_r = deferred_close(r);
            auto intersecting = intersecting_keys(s, first_range, muts);
            auto asserter = assert_that(std::move(r));
            asserter.produces(intersecting).produces_end_of_stream();
            if (forwarding && !first_range.is_singular() && second_range.other_is_before(first_range, dht::ring_position_comparator(*s))) {
                asserter.fast_forward_to(second_range);
                intersecting = intersecting_keys(s, second_range, muts);
                asserter.produces(intersecting).produces_end_of_stream();
            }
        }
    });
    });
}