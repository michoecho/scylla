/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <boost/intrusive/list.hpp>
#include <seastar/core/memory.hh>

class evictable {
    friend class cache_algorithm;
    using lru_link_type = boost::intrusive::list_member_hook<
        boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;
        //boost::intrusive::link_mode<boost::intrusive::safe_link>>;
    lru_link_type _lru_link;
protected:
    // Prevent destruction via evictable pointer. LRU is not aware of allocation strategy.
    ~evictable();
public:
    using hash_type = uint64_t;
    enum class status : uint8_t {
        NEW, COLD, DETACHED, HOT
    };
    status _status = status::NEW;
    uint64_t _size_when_added = 0;
    evictable() = default;
    evictable(evictable&& o) noexcept;
    evictable& operator=(evictable&&) noexcept = default;

    virtual void on_evicted() noexcept = 0;

    virtual size_t size_bytes() const noexcept = 0;
    virtual hash_type cache_hash() const noexcept = 0;

    bool is_linked() const {
        return _lru_link.is_linked();
    }

    void swap(evictable& o) noexcept {
        std::swap(_status, o._status);
        std::swap(_size_when_added, o._size_when_added);
        _lru_link.swap_nodes(o._lru_link);
    }
};

class cache_algorithm {
private:
    friend class evictable;
    using lru_type = boost::intrusive::list<evictable,
        boost::intrusive::member_hook<evictable, evictable::lru_link_type, &evictable::_lru_link>,
        boost::intrusive::constant_time_size<false>>; // we need this to have bi::auto_unlink on hooks.
    lru_type _hot;
    size_t _hot_total = 0;
    lru_type _cold;
    size_t _cold_total = 0;
    static constexpr float MAX_HOT_FRACTION = 0.8;
    void rebalance() noexcept;
public:
    using reclaiming_result = seastar::memory::reclaiming_result;

    ~cache_algorithm();
    void remove(evictable& e) noexcept;
    void add(evictable& e) noexcept;
    void touch(evictable& e) noexcept;

    // Evicts a single element from the LRU
    reclaiming_result evict() noexcept;

    // Evicts all elements.
    // May stall the reactor, use only in tests.
    void evict_all() {
        while (evict() == reclaiming_result::reclaimed_something) {}
    }
};

inline
evictable::evictable(evictable&& o) noexcept {
    if (o._lru_link.is_linked()) {
        auto prev = o._lru_link.prev_;
        o._lru_link.unlink();
        cache_algorithm::lru_type::node_algorithms::link_after(prev, _lru_link.this_ptr());
    }
    _status = o._status;
    _size_when_added = o._size_when_added;
}
