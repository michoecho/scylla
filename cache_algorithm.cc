/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "cache_algorithm.hh"
#include "log.hh"

logging::logger calogger("cache_algorithm");

evictable::~evictable() {
    assert(!_lru_link.is_linked());
}

cache_algorithm::~cache_algorithm() {
    _hot.clear_and_dispose([] (evictable* e) { e->on_evicted(); });
    _cold.clear_and_dispose([] (evictable* e) { e->on_evicted(); });
}

__attribute__ ((optnone))
void cache_algorithm::remove(evictable& e) noexcept {
    calogger.debug("Removing {:#018x}", e.cache_hash());
    switch (e._status) {
    case evictable::status::COLD:
        _cold.erase(_cold.iterator_to(e));
        _cold_total -= e._size_when_added;
        e._status = evictable::status::DETACHED;
        break;
    case evictable::status::HOT:
        _hot.erase(_hot.iterator_to(e));
        _hot_total -= e._size_when_added;
        e._status = evictable::status::DETACHED;
        break;
    default:
        break;
    }
}

__attribute__ ((optnone))
void cache_algorithm::add(evictable& e) noexcept {
    calogger.debug("Adding {:#018x}", e.cache_hash());
    e._size_when_added = e.size_bytes();
    switch (e._status) {
    case evictable::status::COLD:
    case evictable::status::HOT:
        assert("add preconditions not fulfilled" && false);
        break;
    case evictable::status::DETACHED:
        _hot.push_back(e);
        _hot_total += e._size_when_added;
        e._status = evictable::status::HOT;
        rebalance();
        break;
    case evictable::status::NEW:
        _cold.push_back(e);
        _cold_total += e._size_when_added;
        e._status = evictable::status::COLD;
        break;
    }
}

void cache_algorithm::touch(evictable& e) noexcept {
    remove(e);
    add(e);
}

__attribute__ ((optnone))
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

// Evicts a single element from the LRU
__attribute__ ((optnone))
cache_algorithm::reclaiming_result cache_algorithm::evict() noexcept {
    if (!_cold.empty()) [[likely]] {
        evictable& e = _cold.front();
        calogger.debug("Evicting {:#018x}", e.cache_hash());
        e.on_evicted();
        rebalance();
        return reclaiming_result::reclaimed_something;
    } else if (!_hot.empty()) {
        evictable& e = _hot.front();
        calogger.debug("Evicting {:#018x}", e.cache_hash());
        e.on_evicted();
        return reclaiming_result::reclaimed_something;
    } else {
        return reclaiming_result::reclaimed_nothing;
    }
}
