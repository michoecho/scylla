/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "cache_algorithm.hh"
#include "log.hh"
#include "utils/count_min_sketch.hh"
#include <memory>
#include <random>

logging::logger calogger("cache_algorithm");

evictable::~evictable() {
    assert(!_lru_link.is_linked());
}

class wtinylfu_slru : public cache_algorithm_impl {
private:
    cache_algorithm::lru_type _hot;
    cache_algorithm::lru_type _cold;
    cache_algorithm::lru_type _window;
    cache_algorithm::lru_type _garbage;
    utils::count_min_sketch _sketch;
    std::mt19937 _rng{0};

    uint64_t _stats[16] = {0};

    size_t _hot_total = 0;
    size_t _cold_total = 0;
    size_t _window_total = 0;

    size_t _low_watermark = -1;
    size_t _next_watermark = -1;

    float _main_fraction = 0.80;

    uint64_t _time = 0;
    uint64_t _items = 0;
    uint64_t _next_halve = 100000;
    uint64_t _next_watermark_update = 77777;
#if 0
    static constexpr float MAX_MAIN_FRACTION = 0.9;
    static constexpr float COLD_FRACTION = 0.2 * 0.8;
#endif
    void increment_sketch(evictable::hash_type key) noexcept;
    void evict_from_main() noexcept;
    void evict_from_window() noexcept;
    void evict_from_garbage() noexcept;
    void update_watermarks() noexcept;
    void incrementally_rebalance_window() noexcept;
public:
    using reclaiming_result = seastar::memory::reclaiming_result;

    wtinylfu_slru(size_t expected_entries);
    ~wtinylfu_slru() noexcept;
    void remove(evictable& e) noexcept override;
    void add(evictable& e) noexcept override;
    void touch(evictable& e) noexcept override;
    void splice_garbage(cache_algorithm::lru_type& garbage) noexcept override;

    // Evicts a single element from the LRU
    reclaiming_result evict() noexcept override;
};

wtinylfu_slru::wtinylfu_slru(size_t expected_entries)
    : _sketch(expected_entries)
{}

wtinylfu_slru::~wtinylfu_slru() noexcept {
    assert(_window.empty());
    assert(_cold.empty());
    assert(_hot.empty());
    assert(_garbage.empty());
}

void wtinylfu_slru::increment_sketch(evictable::hash_type key) noexcept {
    if ((key >> 60) <= 1) {
        _sketch.increment(key);
    }
    _time += 1;
    if (_time >= _next_halve) {
        _sketch.halve();
        _next_halve = _time + 10 * _items;
    }
    if (_time % 77777 == 0) {
        //calogger.info("key {:#018x} {}", key, _sketch.estimate(key));
    }
}

void wtinylfu_slru::remove(evictable& e) noexcept {
    switch (e._status) {
    case evictable::status::WINDOW:
        _window.erase(_window.iterator_to(e));
        _window_total -= e._size;
        --_items;
        --_stats[(e._hash >> 60)];
        break;
    case evictable::status::COLD:
        _cold.erase(_cold.iterator_to(e));
        _cold_total -= e._size;
        --_items;
        --_stats[(e._hash >> 60)];
#if 0
        incrementally_rebalance_window();
        incrementally_rebalance_main();
#endif
        break;
    case evictable::status::HOT:
        _hot.erase(_hot.iterator_to(e));
        _hot_total -= e._size;
        --_items;
        --_stats[(e._hash >> 60)];
#if 0
        incrementally_rebalance_window();
#endif
        break;
    case evictable::status::GARBAGE:
        if (e._lru_link.is_linked()) {
            e._lru_link.unlink();
        }
        break;
    }
    e._status = evictable::status::GARBAGE;
}

void wtinylfu_slru::add(evictable& e) noexcept {
    assert(!e._lru_link.is_linked());
    assert(e._status == evictable::status::GARBAGE);
    uint64_t size = e.size_bytes();
    e._size = size;
    assert(e._size == size); // Assert that size fits in uint32_t
    e._hash = e.cache_hash();
    _window.push_back(e);
    e._status = evictable::status::WINDOW;
    _window_total += e._size;
    ++_items;
    ++_stats[(e._hash >> 60)];
    increment_sketch(e._hash);
}

void wtinylfu_slru::touch(evictable& e) noexcept {
    switch (e._status) {
    case evictable::status::HOT:
        _hot.erase(_hot.iterator_to(e));
        _hot.push_back(e);
        break;
    case evictable::status::COLD:
        _cold.erase(_cold.iterator_to(e));
        _cold.push_back(e);
#if 0
        _cold_total -= e._size;
        _hot.push_back(e);
        _hot_total += e._size;
        e._status = evictable::status::HOT;
        incrementally_rebalance_main();
#endif
        break;
    case evictable::status::WINDOW:
        _window.erase(_window.iterator_to(e));
        _window.push_back(e);
        break;
    case evictable::status::GARBAGE:
        if (e._lru_link.is_linked()) {
            e._lru_link.unlink();
        }
        add(e);
        return;
    }
    increment_sketch(e._hash);
}

#if 0
[[gnu::noinline]]
void wtinylfu_slru::incrementally_rebalance_main() noexcept {
    for (int i = 0; i < 2; ++i) {
        if (!_hot.empty() && _cold_total <= COLD_FRACTION * _low_watermark) {
            evictable& e = _hot.front();
            _hot.pop_front();
            _hot_total -= e._size;
            _cold.push_back(e);
            _cold_total += e._size;
            e._status = evictable::status::COLD;
        }
    }
}
#endif

[[gnu::noinline]]
void wtinylfu_slru::incrementally_rebalance_window() noexcept {
    for (int i = 0; i < 3; ++i) {
        if (!_window.empty() && (_window.front()._size + _cold_total + _hot_total <= _main_fraction * _low_watermark)) {
            evictable& candidate = _window.front();
            _window.pop_front();
            _window_total -= candidate._size;
            if ((candidate._hash >> 60) >= 2) {
                _garbage.push_front(candidate);
                candidate._status = evictable::status::GARBAGE;
                --_stats[(candidate._hash >> 60)];
                --_items;
            } else {
                _cold.push_front(candidate); // sic! They haven't earned they keep yet.
                _cold_total += candidate._size;
                candidate._status = evictable::status::COLD;
            }
        }
    }
}

void wtinylfu_slru::evict_from_main() noexcept {
    if (!_cold.empty()) {
        evictable& e = _cold.front();
        _cold.pop_front();
        _cold_total -= e._size;
        e._status = evictable::status::GARBAGE;
        --_stats[(e._hash >> 60)];
        e.on_evicted();
        --_items;
    } else {
        assert(!_hot.empty());
        evictable& e = _hot.front();
        _hot.pop_front();
        _hot_total -= e._size;
        e._status = evictable::status::GARBAGE;
        --_stats[(e._hash >> 60)];
        e.on_evicted();
        --_items;
    }
}

void wtinylfu_slru::evict_from_garbage() noexcept {
    evictable& e = _garbage.front();
    _garbage.pop_front();
    e.on_evicted();
}

void wtinylfu_slru::evict_from_window() noexcept {
    if (_cold.empty()) [[unlikely]] {
        // Apparently the background balancer isn't doing its job fast enough.
        std::swap(_cold, _hot);
        std::swap(_cold_total, _hot_total);
    }

    evictable& candidate = _window.front();
    _window.pop_front();
    _window_total -= candidate._size;

    if ((candidate._hash >> 60) >= 2) {
        candidate._status = evictable::status::GARBAGE;
        --_stats[(candidate._hash >> 60)];
        candidate.on_evicted();
        --_items;
        return;
    }

    bool deserves_freebie = candidate._size + _cold_total + _hot_total <= _main_fraction * _low_watermark;
    if (deserves_freebie && std::uniform_int_distribution<int>(0, 1)(_rng) == 0) {
        _cold.push_back(candidate);
        _cold_total += candidate._size;
        candidate._status = evictable::status::COLD;
        return;
    }

    size_t candidate_size = candidate._size;
    size_t candidate_freq = _sketch.estimate(candidate._hash); 
    size_t victim_size = 0;
    size_t victim_freq = 0;

    for (auto it = _cold.begin(); it != _cold.end() && (victim_size < candidate_size); ++it) {
        victim_size += it->_size;
        victim_freq += _sketch.estimate(it->_hash);
        if (victim_freq > candidate_freq) {
            candidate._status = evictable::status::GARBAGE;
            --_stats[(candidate._hash >> 60)];
            candidate.on_evicted();
            --_items;
            return;
        }
    }

    _cold.push_back(candidate);
    _cold_total += candidate_size;
    candidate._status = evictable::status::COLD;

    size_t target_size = _cold_total - victim_size;
    while (_cold_total > target_size) {
        evictable& victim = _cold.front();
        _cold.pop_front();
        _cold_total -= victim._size;
        victim._status = evictable::status::GARBAGE;
        --_stats[(victim._hash >> 60)];
        victim.on_evicted();
        --_items;
    }
}

void wtinylfu_slru::update_watermarks() noexcept {
    size_t current_total = _cold_total + _window_total + _hot_total;
    _next_watermark = std::min(_next_watermark, current_total);
    _low_watermark = std::min(_low_watermark, _next_watermark);
    if (_time >= _next_watermark_update) [[unlikely]] {
        _low_watermark = _next_watermark;
        _next_watermark = -1;
        _next_watermark_update = _time + 0.1 * _items;
        calogger.info("watermark update: {} {} {} {} {}", _low_watermark, _window_total, _hot_total, _cold_total, _items);
        calogger.info("stats: {} {} {} {}", _stats[0], _stats[1], _stats[2], _stats[3]);
    }
}

cache_algorithm::reclaiming_result wtinylfu_slru::evict() noexcept {
    update_watermarks();

    if (!_garbage.empty()) [[unlikely]] {
        evict_from_garbage();
        return reclaiming_result::reclaimed_something;
    }

    size_t current_total = _cold_total + _window_total + _hot_total;
    if (current_total == 0) {
        return reclaiming_result::reclaimed_nothing;
    }

    if (_window.empty() || _cold_total + _hot_total > _main_fraction * _low_watermark) {
        // Can happen after a bump in memory pressure (which shrunk _low_watermark).
        evict_from_main();
        return reclaiming_result::reclaimed_something;
    }

    evict_from_window();
    return reclaiming_result::reclaimed_something;
}

void wtinylfu_slru::splice_garbage(cache_algorithm::lru_type& garbage) noexcept {
    _garbage.splice(_garbage.end(), garbage);
}

cache_algorithm::cache_algorithm()
    : _impl(std::make_unique<wtinylfu_slru>(1000000)) {}

void cache_algorithm::evict_all() noexcept {
    while (evict() == reclaiming_result::reclaimed_something) {}
}
