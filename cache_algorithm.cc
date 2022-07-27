/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#if 0
#pragma clang optimize off
#endif

#include "cache_algorithm.hh"
#include "log.hh"
#include "utils/count_min_sketch.hh"
#include <memory>
#include <random>

logging::logger calogger("cache_algorithm");

void cache_algorithm::evict_all() noexcept {
    while (evict() == reclaiming_result::reclaimed_something) {}
}

evictable::~evictable() {
    assert(!_lru_link.is_linked());
    assert(_status == evictable::status::GARBAGE);
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

    float _main_fraction = 0.99;

    uint64_t _time = 0;
    uint64_t _items = 0;
    uint64_t _next_halve = 100000;
    uint64_t _next_watermark_update = 77777;

    static constexpr float COLD_FRACTION = 0.2 * 0.8;

    void increment_sketch(evictable::hash_type key) noexcept;
    void evict_worst() noexcept;
    void update_watermarks() noexcept;
    void evict_item(evictable& e) noexcept;
    evictable& pop_cold() noexcept;
    evictable& pop_hot() noexcept;
    evictable& pop_window() noexcept;
    void push_cold(evictable& e) noexcept;
    void push_hot(evictable& e) noexcept;
    void push_window(evictable& e) noexcept;
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

bool is_cached_type(evictable::hash_type key) {
    return (key >> 60) <= 1;
}

void wtinylfu_slru::increment_sketch(evictable::hash_type key) noexcept {
    if (is_cached_type(key)) {
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
        break;
    case evictable::status::HOT:
        _hot.erase(_hot.iterator_to(e));
        _hot_total -= e._size;
        --_items;
        --_stats[(e._hash >> 60)];
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
    push_window(e);
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
        _cold_total -= e._size;
        push_hot(e);
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

void wtinylfu_slru::evict_item(evictable &e) noexcept {
    e._status = evictable::status::GARBAGE;
    --_stats[(e._hash >> 60)];
    e.on_evicted();
    --_items;
}

evictable& wtinylfu_slru::pop_cold() noexcept {
    evictable& e= _cold.front();
    _cold.pop_front();
    _cold_total -= e._size;
    return e;
}

evictable& wtinylfu_slru::pop_hot() noexcept {
    evictable& e = _hot.front();
    _hot.pop_front();
    _hot_total -= e._size;
    return e;
}

evictable& wtinylfu_slru::pop_window() noexcept {
    evictable& e = _window.front();
    _window.pop_front();
    _window_total -= e._size;
    return e;
}

void wtinylfu_slru::push_cold(evictable& e) noexcept {
    _cold_total += e._size;
    _cold.push_back(e);
    e._status = evictable::status::COLD;
}

void wtinylfu_slru::push_hot(evictable& e) noexcept {
    _hot_total += e._size;
    _hot.push_back(e);
    e._status = evictable::status::HOT;
}

void wtinylfu_slru::push_window(evictable& e) noexcept {
    _window_total += e._size;
    _window.push_back(e);
    e._status = evictable::status::WINDOW;
}

void wtinylfu_slru::update_watermarks() noexcept {
    size_t current_total = _cold_total + _window_total + _hot_total;
    _next_watermark = std::min(_next_watermark, current_total);
    _low_watermark = std::min(_low_watermark, _next_watermark);
    if (_time >= _next_watermark_update) [[unlikely]] {
        _low_watermark = _next_watermark;
        _next_watermark = -1;
        _next_watermark_update = _time + 10 * _items;
        calogger.info("watermark update: {} {} {} {} {}", _low_watermark, _window_total, _hot_total, _cold_total, _items);
        calogger.info("stats: {} {} {} {}", _stats[0], _stats[1], _stats[2], _stats[3]);
    }
}

cache_algorithm::reclaiming_result wtinylfu_slru::evict() noexcept {
    constexpr int BATCH_SIZE = 1;
    update_watermarks();

    if (!_garbage.empty()) [[unlikely]] {
        for (int i = 0; i < BATCH_SIZE; ++i) {
            if (!_garbage.empty()) {
                evictable& e = _garbage.front();
                _garbage.pop_front();
                e.on_evicted();
            }
        }
        return reclaiming_result::reclaimed_something;
    }

    if (_hot_total && _hot_total > _low_watermark * (_main_fraction - COLD_FRACTION)) {
        for (int i = 0; i < BATCH_SIZE; ++i) {
            if (_hot_total && _hot_total > _low_watermark * (_main_fraction - COLD_FRACTION)) {
                evictable& candidate = pop_hot();
                push_cold(candidate);
            }
        }
    }
    if (_cold_total + _hot_total < _main_fraction * _low_watermark) {
        // Theoretically may not converge to the goal
        // (if window perfectly interleaves BATCH_SIZE index items and 1 non-index item),
        // but we ignore that for now.
        for (int i = 0; i < BATCH_SIZE; ++i) {
            if (_window_total) {
                if (!is_cached_type(_window.front()._hash)) {
                    evictable& candidate = pop_window();
                    evict_item(candidate);
                } else if (_cold_total + _hot_total + _window.front()._size <= _main_fraction * _low_watermark) {
                    evictable& candidate = pop_window();
                    push_cold(candidate);
                } else {
                    break;
                }
            }
        }
    } else if (_cold_total + _hot_total > _main_fraction * _low_watermark) {
        for (int i = 0; i < BATCH_SIZE; ++i) {
            if (_cold_total && _cold_total + _hot_total > _main_fraction) {
                evictable& candidate = pop_cold();
                evict_item(candidate);
            }
        }
        return reclaiming_result::reclaimed_something;
    }
    if (!_window_total) [[unlikely]] {
        if (_cold_total) {
            evictable& candidate = pop_cold();
            evict_item(candidate);
            return reclaiming_result::reclaimed_something;
        } else if (_hot_total) {
            evictable& candidate = pop_hot();
            evict_item(candidate);
            return reclaiming_result::reclaimed_something;
        } else {
            return reclaiming_result::reclaimed_nothing;
        }
    }
    for (int i = 0; i < BATCH_SIZE; ++i) {{
        if (!_window_total) {
            return reclaiming_result::reclaimed_nothing;
            break;
        }
        evictable& candidate = pop_window();

        if (!is_cached_type(candidate._hash)) {
            evict_item(candidate);
            goto cnt;
        }

        size_t candidate_size = candidate._size;
        size_t candidate_freq = _sketch.estimate(candidate._hash); 
        size_t victim_size = 0;
        size_t victim_freq = 0;

        for (auto it = _cold.begin(); it != _cold.end() && (victim_size < candidate_size); ++it) {
            victim_size += it->_size;
            victim_freq += _sketch.estimate(it->_hash);
            if (victim_freq > candidate_freq || true) {
                evict_item(candidate);
                goto cnt;
            }
        }

        push_cold(candidate);

        size_t target_size = _cold_total - victim_size;
        while (_cold_total > target_size) {
            evictable& victim = pop_cold();
            evict_item(victim);
        }
    }cnt:;}

    return reclaiming_result::reclaimed_something;
}

void wtinylfu_slru::splice_garbage(cache_algorithm::lru_type& garbage) noexcept {
    _garbage.splice(_garbage.end(), garbage);
}

#if 0
class lirs : public cache_algorithm_impl {
private:
    cache_algorithm::lru_type _s;
    cache_algorithm::lru_type _q;
    cache_algorithm::lru_type _q;

    uint64_t _stats[16] = {0};

    size_t q_max_size = 100 * 1024 * 1024;

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
#endif

cache_algorithm::cache_algorithm()
    : _impl(std::make_unique<wtinylfu_slru>(1000000)) {}
