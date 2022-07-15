/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "cache_algorithm.hh"

evictable::~evictable() {
    assert(!_lru_link.is_linked());
}

cache_algorithm::~cache_algorithm() {
    _list.clear_and_dispose([] (evictable* e) {
        e->on_evicted();
    });
}

void cache_algorithm::remove(evictable& e) noexcept {
    if (e.is_linked()) {
        _list.erase(_list.iterator_to(e));
    }
}

void cache_algorithm::add(evictable& e) noexcept {
    _list.push_back(e);
}

void cache_algorithm::touch(evictable& e) noexcept {
    remove(e);
    add(e);
}

void cache_algorithm::rebalance() noexcept {
    while (_hot_total > MAX_HOT_FRACTION * (_hot_total + _cold_total)) {
        evictable& e = _hot.front();
        _hot.pop_front();
        _hot_total -= e._size_when_added;
        _cold.push_back(e);
        _cold_total += e._size_when_added;
        e._status = evictable::status::COLD;
    }
}

cache_algorithm::reclaiming_result cache_algorithm::evict() noexcept {
    if (_list.empty()) {
        return reclaiming_result::reclaimed_nothing;
    }
    evictable& e = _list.front();
    _list.pop_front();
    e.on_evicted();
    return reclaiming_result::reclaimed_something;
}
