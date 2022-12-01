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
    friend class lru;
    // For bookkeeping, we want the unlinking of evictables to be explicit.
    // E.g. if the cache's internal data structure consists of multiple lists, we would
    // like to know which list is an element being removed from.
    // Therefore, we are using auto_unlink only to be able to call unlink() in the move constructor
    // and we do NOT rely on automatic unlinking in _lru_link's destructor.
    // It's the programmer's responsibility. to call lru::remove on the evictable before its destruction.
    // Failure to do so is a bug, and it will trigger an assertion in the destructor.
protected:
    using link_base = boost::intrusive::list_member_hook<boost::intrusive::link_mode<boost::intrusive::auto_unlink>>;
    struct lru_link_type : link_base {
        lru_link_type() noexcept = default;
        lru_link_type(lru_link_type&& o) noexcept {
            swap_nodes(o);
        }
    };
    static_assert(std::is_nothrow_constructible_v<lru_link_type, lru_link_type&&>);
private:
    lru_link_type _lru_link;
protected:
    // Prevent destruction via evictable pointer. LRU is not aware of allocation strategy.
    // Prevent destruction of a linked evictable. While we could unlink the evictable here
    // in the destructor, we can't perform proper accounting for that without access to the
    // head of the containing list.
    ~evictable() {
        assert(!_lru_link.is_linked());
    }
    evictable() = default;
    evictable(evictable&&) = default;
public:
    virtual void on_evicted() noexcept = 0;

    bool is_linked() const {
        return _lru_link.is_linked();
    }

    void swap(evictable& o) noexcept {
        _lru_link.swap_nodes(o._lru_link);
    }

    virtual bool is_index() const noexcept {
        return false;
    }
};

class index_evictable : public evictable {
    friend class lru;
    evictable::lru_link_type _index_lru_link;
    bool is_index() const noexcept override {
        return true;
    }
    void swap(evictable& o) noexcept = delete;
};

class lru {
private:
    using lru_type = boost::intrusive::list<evictable,
        boost::intrusive::member_hook<evictable, evictable::lru_link_type, &evictable::_lru_link>,
        boost::intrusive::constant_time_size<false>>; // we need this to have bi::auto_unlink on hooks.
    using index_lru_type = boost::intrusive::list<index_evictable,
        boost::intrusive::member_hook<index_evictable, index_evictable::lru_link_type, &index_evictable::_index_lru_link>,
        boost::intrusive::constant_time_size<false>>; // we need this to have bi::auto_unlink on hooks.
    lru_type _list;
    index_lru_type _index_list;

    using reclaiming_result = seastar::memory::reclaiming_result;

public:
    ~lru() {
        while (!_list.empty()) {
            evictable& e = _list.front();
            remove(e);
            e.on_evicted();
        }
    }

    void remove(evictable& e) noexcept {
        _list.erase(_list.iterator_to(e));
        if (e.is_index()) {
            _index_list.erase(_index_list.iterator_to(static_cast<index_evictable&>(e)));
        }
    }

    void add(evictable& e) noexcept {
        _list.push_back(e);
        if (e.is_index()) {
            _index_list.push_back(static_cast<index_evictable&>(e));
        }
    }

    void touch(evictable& e) noexcept {
        remove(e);
        add(e);
    }

    // Evicts a single element from the LRU
    reclaiming_result evict(bool should_evict_index = false) noexcept {
        if (_list.empty()) {
            return reclaiming_result::reclaimed_nothing;
        }
        evictable& e = (should_evict_index && !_index_list.empty()) ? _index_list.front() : _list.front();
        remove(e);
        e.on_evicted();
        return reclaiming_result::reclaimed_something;
    }

    // Evicts all elements.
    // May stall the reactor, use only in tests.
    void evict_all() {
        while (evict() == reclaiming_result::reclaimed_something) {}
    }
};
