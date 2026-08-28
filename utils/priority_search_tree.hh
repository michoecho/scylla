/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "utils/assert.hh"

namespace utils {

// An index of closed intervals [start, end] which answers interval overlap
// queries. The tree is ordered by (start, end), balanced by height, and each
// node stores the largest end in its subtree. The latter is the priority-search
// part: a subtree whose maximum end is below the query's lower bound cannot
// contain a match and can be skipped.
//
// Point queries cost O(log n + matches), and range queries cost
// O(log n + matches). Values are opaque to the index; equal intervals and
// equal (interval, value) pairs may be inserted more than once.
template <std::integral Key, typename Value, typename Compare = std::less<Key>>
class priority_search_tree {
    struct node;
    using node_ptr = std::unique_ptr<node>;

    struct node {
        Key start;
        Key end;
        Key max_end;
        Value value;
        node_ptr left;
        node_ptr right;
        unsigned height = 1;

        node(Key start, Key end, Value value)
                : start(std::move(start))
                , end(std::move(end))
                , max_end(this->end)
                , value(std::move(value)) {
        }
    };

    node_ptr _root;
    size_t _size = 0;
    Compare _cmp;

private:
    static unsigned height(const node_ptr& n) noexcept {
        return n ? n->height : 0;
    }

    bool less(const Key& a, const Key& b) const {
        return _cmp(a, b);
    }

    bool entry_less(const Key& start, const Key& end, const node& n) const {
        return less(start, n.start) || (!less(n.start, start) && less(end, n.end));
    }

    int compare_entry(const Key& start, const Key& end, const node& n) const {
        if (entry_less(start, end, n)) {
            return -1;
        }
        if (less(n.start, start) || (!less(start, n.start) && less(n.end, end))) {
            return 1;
        }
        return 0;
    }

    void update(node& n) const {
        n.height = 1 + std::max(height(n.left), height(n.right));
        n.max_end = n.end;
        if (n.left && less(n.max_end, n.left->max_end)) {
            n.max_end = n.left->max_end;
        }
        if (n.right && less(n.max_end, n.right->max_end)) {
            n.max_end = n.right->max_end;
        }
    }

    static int balance_factor(const node& n) noexcept {
        return int(height(n.left)) - int(height(n.right));
    }

    node_ptr rotate_left(node_ptr n) const {
        auto new_root = std::move(n->right);
        n->right = std::move(new_root->left);
        update(*n);
        new_root->left = std::move(n);
        update(*new_root);
        return new_root;
    }

    node_ptr rotate_right(node_ptr n) const {
        auto new_root = std::move(n->left);
        n->left = std::move(new_root->right);
        update(*n);
        new_root->right = std::move(n);
        update(*new_root);
        return new_root;
    }

    node_ptr rebalance(node_ptr n) const {
        update(*n);
        auto bf = balance_factor(*n);
        if (bf > 1) {
            if (balance_factor(*n->left) < 0) {
                n->left = rotate_left(std::move(n->left));
            }
            return rotate_right(std::move(n));
        }
        if (bf < -1) {
            if (balance_factor(*n->right) > 0) {
                n->right = rotate_right(std::move(n->right));
            }
            return rotate_left(std::move(n));
        }
        return n;
    }

    void insert(node_ptr& n, Key start, Key end, Value value) {
        if (!n) {
            n = std::make_unique<node>(std::move(start), std::move(end), std::move(value));
            return;
        }
        if (entry_less(start, end, *n)) {
            insert(n->left, std::move(start), std::move(end), std::move(value));
        } else {
            insert(n->right, std::move(start), std::move(end), std::move(value));
        }
        n = rebalance(std::move(n));
    }

    node_ptr clone(const node* n) const {
        if (!n) {
            return {};
        }
        auto copy = std::make_unique<node>(n->start, n->end, n->value);
        copy->max_end = n->max_end;
        copy->height = n->height;
        copy->left = clone(n->left.get());
        copy->right = clone(n->right.get());
        return copy;
    }

    node_ptr extract_min(node_ptr n, node_ptr& min) const {
        if (!n->left) {
            min = std::move(n);
            auto right = std::move(min->right);
            min->right.reset();
            return right;
        }
        n->left = extract_min(std::move(n->left), min);
        return rebalance(std::move(n));
    }

    node_ptr remove_node(node_ptr n) const {
        if (!n->left) {
            return std::move(n->right);
        }
        if (!n->right) {
            return std::move(n->left);
        }

        node_ptr successor;
        auto right = extract_min(std::move(n->right), successor);
        successor->left = std::move(n->left);
        successor->right = std::move(right);
        return rebalance(std::move(successor));
    }

    node_ptr erase(node_ptr n, const Key& start, const Key& end, const Value& value, bool& erased) {
        if (!n) {
            return {};
        }

        auto cmp = compare_entry(start, end, *n);
        if (cmp < 0) {
            n->left = erase(std::move(n->left), start, end, value, erased);
        } else if (cmp > 0) {
            n->right = erase(std::move(n->right), start, end, value, erased);
        } else if (n->value == value) {
            erased = true;
            return remove_node(std::move(n));
        } else {
            // Rotations can put equal (start, end) entries on either side of
            // this node, so search both sides when this value is not a match.
            n->left = erase(std::move(n->left), start, end, value, erased);
            if (!erased) {
                n->right = erase(std::move(n->right), start, end, value, erased);
            }
        }
        return rebalance(std::move(n));
    }

    template <typename Fn>
    void for_each_overlapping(const node* n, const Key& low, const Key& high, Fn& f) const {
        if (!n || less(n->max_end, low)) {
            return;
        }
        if (!less(high, n->start) && !less(n->end, low)) {
            f(n->value);
        }
        // The left subtree may contain entries with starts before this node;
        // it is pruned only by its own max_end summary.
        for_each_overlapping(n->left.get(), low, high, f);
        // Every entry in the right subtree starts at or after n->start.
        if (!less(high, n->start)) {
            for_each_overlapping(n->right.get(), low, high, f);
        }
    }

    void next_start_after(const node* n, const Key& pos, std::optional<Key>& next) const {
        if (!n) {
            return;
        }
        if (less(pos, n->start)) {
            if (!next || less(n->start, *next)) {
                next = n->start;
            }
            next_start_after(n->left.get(), pos, next);
        } else {
            next_start_after(n->right.get(), pos, next);
        }
    }

    void next_close_after(const node* n, const Key& pos, std::optional<Key>& next) const {
        if (!n || less(n->max_end, pos)) {
            return;
        }
        if (!less(pos, n->start) && !less(n->end, pos)
                && n->end != std::numeric_limits<Key>::max()) {
            auto close = n->end + 1;
            if (!next || less(close, *next)) {
                next = close;
            }
        }
        next_close_after(n->left.get(), pos, next);
        if (!less(pos, n->start)) {
            next_close_after(n->right.get(), pos, next);
        }
    }

public:
    priority_search_tree() = default;

    priority_search_tree(const priority_search_tree& other)
            : _root(clone(other._root.get()))
            , _size(other._size)
            , _cmp(other._cmp) {
    }

    priority_search_tree& operator=(const priority_search_tree& other) {
        if (this != &other) {
            auto root = clone(other._root.get());
            _root = std::move(root);
            _size = other._size;
            _cmp = other._cmp;
        }
        return *this;
    }

    priority_search_tree(priority_search_tree&&) noexcept = default;
    priority_search_tree& operator=(priority_search_tree&&) noexcept = default;

    size_t size() const noexcept { return _size; }
    bool empty() const noexcept { return !_root; }

    void clear() noexcept {
        _root.reset();
        _size = 0;
    }

    // Adds the closed interval [start, end].
    void insert(Key start, Key end, Value value) {
        SCYLLA_ASSERT(!less(end, start));
        insert(_root, std::move(start), std::move(end), std::move(value));
        ++_size;
    }

    // Removes one entry equal to (start, end, value), if present.
    bool erase(const Key& start, const Key& end, const Value& value) {
        bool erased = false;
        _root = erase(std::move(_root), start, end, value, erased);
        if (erased) {
            --_size;
        }
        return erased;
    }

    template <typename Fn>
    requires std::invocable<Fn, const Value&>
    void for_each_overlapping(const Key& low, const Key& high, Fn&& f) const {
        if (less(high, low)) {
            return;
        }
        for_each_overlapping(_root.get(), low, high, f);
    }

    template <typename Fn>
    requires std::invocable<Fn, const Value&>
    void for_each_covering(const Key& pos, Fn&& f) const {
        for_each_overlapping(pos, pos, std::forward<Fn>(f));
    }

    // Returns the first position after `pos` at which the set of covering
    // intervals can change. An opening interval contributes its start, and a
    // covering interval contributes the position after its end.
    std::optional<Key> next_change(const Key& pos) const {
        std::optional<Key> next;
        next_start_after(_root.get(), pos, next);
        next_close_after(_root.get(), pos, next);
        return next;
    }
};

}
