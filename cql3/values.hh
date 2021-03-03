/*
 * Copyright (C) 2017 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "bytes.hh"

#include <optional>
#include <variant>

#include <seastar/util/variant_utils.hh>

#include "utils/fragmented_temporary_buffer.hh"
#include "utils/managed_bytes.hh"

namespace cql3 {

class null_value {};
class unset_value {};

class raw_value_view;

/// \brief Raw CQL protocol value.
///
/// The `raw_value` type represents an uninterpreted value from the CQL wire
/// protocol. A raw value can hold either a null value, an unset value, or a byte
/// blob that represents the value.
class raw_value {
    std::variant<managed_bytes, bytes, null_value, unset_value> _data;

    raw_value(null_value data)
        : _data{data}
    {}
    raw_value(unset_value data)
        : _data{data}
    {}
    raw_value(bytes&& data)
        : _data{std::move(data)}
    {}
    raw_value(managed_bytes&& data)
        : _data{std::move(data)}
    {}
public:
    static raw_value make_null() {
        return raw_value{null_value{}};
    }
    static raw_value make_unset_value() {
        return raw_value{unset_value{}};
    }
#if 0
    static raw_value make_value(const raw_value_view& view);
#endif
    static raw_value make_value(managed_bytes&& b) {
        return raw_value{std::move(b)};
    }
    static raw_value make_value(const managed_bytes& b) {
        return raw_value{managed_bytes(b)};
    }
    static raw_value make_value(const std::optional<managed_bytes>& b) {
        if (b) {
            return make_value(*b);
        }
        return make_null();
    }
    static raw_value make_value(bytes&& b) {
        return raw_value{std::move(b)};
    }
    static raw_value make_value(const bytes& b) {
        return raw_value{bytes(b)};
    }
    static raw_value make_value(const bytes_opt& b) {
        if (b) {
            return make_value(*b);
        }
        return make_null();
    }
    bool is_null() const {
        return std::holds_alternative<null_value>(_data);
    }
    bool is_unset_value() const {
        return std::holds_alternative<unset_value>(_data);
    }
    bool is_value() const {
        return _data.index() <= 1;
    }
    explicit operator bool() const {
        return is_value();
    }
    raw_value_view to_view() const;
    bytes to_bytes() const {
        if (auto p = std::get_if<bytes>(&_data)) {
            return *p;
        } else {
            return ::to_bytes(std::get<managed_bytes>(_data));
        }
    }
    friend class raw_value_view;
};

/// \brief View to a raw CQL protocol value.
///
/// \see raw_value
class raw_value_view {
public:
    class view {
    public:
        using fragment_type = managed_bytes_view::fragment_type;
    private:
        // We use managed_bytes as a general-purpose fragmented buffer,
        // so we want raw_value_view to handle managed_bytes_view,
        // but we also want raw_value_view to handle fragmented_temporary_buffer::view without copying,
        // because that's what's coming from the wire. Therefore, raw_value_view is a
        // variant of managed_bytes_view and fragmented_temporary_buffer::view.
        //
        // Since the only part that differs between fragmented_temporary_buffer::view
        // and managed_bytes_view is the method of getting the next fragment, which happens rarely,
        // and the frequently used parts (accessing the first fragment and the total size)
        // are common for both view types, we don't use a std::variant, and instead we store the
        // common parts directly in raw_value_view, and only dispatch the action of getting the next
        // fragment to the actual view type.
        
        using mb = managed_bytes_view::next_fragments_opaque;
        using ftbv = fragmented_temporary_buffer::view::next_fragments_opaque;

        std::variant<mb, ftbv, null_value, unset_value> _content;
        fragment_type _current_fragment;
        size_t _size = 0;

        view(null_value data) : _content(data) {}
        view(unset_value data) : _content(data) {}
        view(const fragmented_temporary_buffer::view& data)
            : _content(data.next_fragments())
            , _current_fragment{data.current_fragment()}
            , _size{data.size_bytes()}
        {}

    public:
        view(const managed_bytes_view& data)
            : _content(data.next_fragments())
            , _current_fragment{data.current_fragment()}
            , _size{data.size_bytes()}
        {}

        view() : view(unset_value()) {}

        // Implements FragmentedView
        size_t size_bytes() const { return _size; }
        fragment_type current_fragment() const { return _current_fragment; }
        bool empty() const { return _size == 0; }
        void remove_prefix(size_t n) noexcept {
            while (n >= _current_fragment.size() && n > 0) [[unlikely]] {
                n -= _current_fragment.size();
                remove_current();
            }
            _size -= n;
            _current_fragment.remove_prefix(n);
        }
        void remove_current() {
            _size -= _current_fragment.size();
            if (_size) {
                if (auto p = std::get_if<mb>(&_content)) {
                    _current_fragment = managed_bytes_view::get_next_fragment(*p);
                } else {
                    _current_fragment = fragmented_temporary_buffer::view::get_next_fragment(std::get<ftbv>(_content));
                }
                _current_fragment = _current_fragment.substr(0, _size);
            } else {
                _current_fragment = fragment_type();
            }
        }
        view prefix(size_t len) const {
            view v = *this;
            v._size = len;
            v._current_fragment = v._current_fragment.substr(0, len);
            return v;
        }
        friend class raw_value_view;
    };
private:
    // Temporary storage is only useful if a raw_value_view needs to be instantiated
    // with a value which lifetime is bounded only to the view itself.
    // This hack is introduced in order to avoid storing temporary storage
    // in an external container, which may cause memory leaking problems.
    // This pointer is disengaged for regular raw_value_view instances.
    // Data is stored in a shared pointer for two reasons:
    // - pointers are cheap to copy
    // - it makes the view keep its semantics - it's safe to copy a view multiple times
    //   and all copies still refer to the same underlying data.
    lw_shared_ptr<raw_value> _temporary_storage = nullptr;
    view _view;

    // This constructor is only used by make_temporary() and it acquires ownership
    // of the given buffer. The view created that way refers to its own temporary storage.
    explicit raw_value_view(raw_value&& temporary_storage);

public:
    raw_value_view(view data) : _view(data) {}
    static raw_value_view make_null() {
        return raw_value_view(view(null_value()));
    }
    static raw_value_view make_unset_value() {
        return raw_value_view(view(unset_value()));
    }
    static raw_value_view make_value(const fragmented_temporary_buffer::view& v) {
        return raw_value_view(view(v));
    }
    static raw_value_view make_value(const managed_bytes_view& v) {
        return raw_value_view(view(v));
    }
    static raw_value_view make_temporary(raw_value&& value) {
        return raw_value_view(std::move(value));
    }
    bool is_null() const {
        return std::holds_alternative<null_value>(_view._content);
    }
    bool is_unset_value() const {
        return std::holds_alternative<unset_value>(_view._content);
    }
    bool is_value() const {
        return std::holds_alternative<view::mb>(_view._content)
            || std::holds_alternative<view::ftbv>(_view._content);
    }
    explicit operator bool() const {
        return is_value();
    }
    const view* operator->() const {
        return &_view;
    }
    const view& operator*() const {
        return _view;
    }

    bool operator==(const raw_value_view& other) const {
        if (is_value() == other.is_value()) {
            return equal_unsigned(_view, other._view);
        } else {
            return is_null() == other.is_null();
        }
    }

    friend std::ostream& operator<<(std::ostream& os, const raw_value_view& value);
    static view extract_view(const raw_value& v);
};

}

inline bytes to_bytes(const cql3::raw_value& v)
{
    return v.to_bytes();
}

inline bytes to_bytes(const cql3::raw_value_view& view)
{
    return linearized(*view);
}

inline bytes_opt to_bytes_opt(const cql3::raw_value_view& view) {
    if (view.is_value()) {
        return bytes_opt(linearized(*view));
    }
    return bytes_opt();
}

inline bytes_opt to_bytes_opt(const cql3::raw_value& value) {
    return to_bytes_opt(value.to_view());
}
