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
        WINDOW, COLD, HOT, GARBAGE
    };
    status _status = status::COLD;
    uint64_t _size_when_added = 0;
protected:
    // Prevent destruction via evictable pointer. LRU is not aware of allocation strategy.
    ~evictable();
public:
    using hash_type = uint64_t;
    evictable() = default;
    evictable(evictable&& o) noexcept;
    evictable& operator=(evictable&&) noexcept = default;

    virtual void on_evicted() noexcept = 0;
    virtual size_t size_bytes() const noexcept = 0;
    virtual hash_type cache_hash() const noexcept = 0;

    bool is_garbage() const {
        return _status == status::GARBAGE;
    }

    bool is_linked() const {
        return _lru_link.is_linked();
    }

    void swap(evictable& o) noexcept {
        std::swap(_status, o._status);
        std::swap(_size_when_added, o._size_when_added);
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
    void remove_garbage(evictable& e) noexcept;
    void splice_garbage(lru_type& garbage) noexcept;
    reclaiming_result evict() noexcept;
    void evict_all() noexcept;
};

class cache_algorithm_impl {
public:
    virtual void remove(evictable& e) noexcept = 0;
    virtual void add(evictable& e) noexcept = 0;
    virtual void touch(evictable& e) noexcept = 0;
    virtual void remove_garbage(evictable& e) noexcept = 0;
    virtual void splice_garbage(cache_algorithm::lru_type& garbage) noexcept = 0;
    virtual cache_algorithm::reclaiming_result evict() noexcept = 0;
};

inline void cache_algorithm::remove(evictable& e) noexcept { _impl->remove(e); }
inline void cache_algorithm::add(evictable& e) noexcept { _impl->add(e); }
inline void cache_algorithm::touch(evictable& e) noexcept { _impl->touch(e); } 
inline void cache_algorithm::remove_garbage(evictable& e) noexcept { _impl->remove_garbage(e); }
inline void cache_algorithm::splice_garbage(lru_type& garbage) noexcept { _impl->splice_garbage(garbage); }
inline cache_algorithm::reclaiming_result cache_algorithm::evict() noexcept { return _impl->evict(); };

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
