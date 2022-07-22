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
    friend class wtinylfu_slru;
    using lru_link_type = boost::intrusive::list_member_hook<
        boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;
    lru_link_type _lru_link;
    enum class status : uint8_t {
        GARBAGE, WINDOW, COLD, HOT
    };
    uint64_t _hash = 0;
    uint32_t _size = 0;
    status _status = status::GARBAGE;
protected:
    // Prevent destruction via evictable pointer. LRU is not aware of allocation strategy.
    ~evictable();
public:
    using hash_type = uint64_t;
    evictable() = default;
    evictable(evictable&& o) noexcept;
    evictable& operator=(evictable&&) noexcept = default;

    // Called from evict().
    // When on_evicted() is called, the evictable is already detached from the algorithm.
    // The implementation should destroy the item immediately if possible,
    // since the purpose of eviction is to reclaim memory.
    // But if destruction is impossible (because the item is currently in use)
    // the implementation can stash a reference to the item, and later destroy it
    // or re-add it to the algorithm.
    // The item can be re-added with add() or touch().
    // If the item is linked into a list, the touch() call will automatically unlink it.
    // During on_evicted, the implementation is forbidden from calling back
    // into the algorithm. (So any re-adding has to occur later, outside of this call.)
    virtual void on_evicted() noexcept = 0;
    virtual size_t size_bytes() const noexcept = 0;
    virtual hash_type cache_hash() const noexcept = 0;

    bool is_garbage() const {
        return _status == status::GARBAGE;
    }

    void swap(evictable& o) noexcept {
        std::swap(_status, o._status);
        std::swap(_size, o._size);
        std::swap(_hash, o._hash);
        _lru_link.swap_nodes(o._lru_link);
    }
};

class cache_algorithm_impl;

class cache_algorithm {
private:
    std::unique_ptr<cache_algorithm_impl> _impl;
public:
    using lru_type = boost::intrusive::list<evictable,
        boost::intrusive::member_hook<evictable, evictable::lru_link_type, &evictable::_lru_link>,
        boost::intrusive::constant_time_size<false>>; // we need this to have bi::auto_unlink on hooks.
    using reclaiming_result = seastar::memory::reclaiming_result;

    cache_algorithm();
    void remove(evictable& e) noexcept;
    void add(evictable& e) noexcept;
    void touch(evictable& e) noexcept;
    void splice_garbage(lru_type& garbage) noexcept;
    reclaiming_result evict() noexcept;
    void evict_all() noexcept;
};

class cache_algorithm_impl {
public:
    virtual void remove(evictable& e) noexcept = 0;
    virtual void add(evictable& e) noexcept = 0;
    virtual void touch(evictable& e) noexcept = 0;
    virtual void splice_garbage(cache_algorithm::lru_type& garbage) noexcept = 0;
    virtual cache_algorithm::reclaiming_result evict() noexcept = 0;
};

inline void cache_algorithm::remove(evictable& e) noexcept { _impl->remove(e); }
inline void cache_algorithm::add(evictable& e) noexcept { _impl->add(e); }
inline void cache_algorithm::touch(evictable& e) noexcept { _impl->touch(e); } 
inline void cache_algorithm::splice_garbage(lru_type& garbage) noexcept { _impl->splice_garbage(garbage); }
inline cache_algorithm::reclaiming_result cache_algorithm::evict() noexcept { return _impl->evict(); };

inline
evictable::evictable(evictable&& o) noexcept {
    if (o._lru_link.is_linked()) {
        auto prev = o._lru_link.prev_;
        o._lru_link.unlink();
        cache_algorithm::lru_type::node_algorithms::link_after(prev, _lru_link.this_ptr());
    }
    _hash = o._hash;
    _status = o._status;
    _size = o._size;
}
