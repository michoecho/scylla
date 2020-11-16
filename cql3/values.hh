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

#include "types.hh"
#include "bytes.hh"

#include <optional>
#include <variant>

#include <seastar/util/variant_utils.hh>

#include "utils/fragmented_temporary_buffer.hh"

namespace cql3 {

struct null_value {
};

struct unset_value {
};

class raw_value;
/// \brief View to a raw CQL protocol value.
///
/// \see raw_value
struct raw_value_view {
    std::variant<managed_bytes_view, fragmented_temporary_buffer::view, null_value, unset_value> _data;
    // Temporary storage is only useful if a raw_value_view needs to be instantiated
    // with a value which lifetime is bounded only to the view itself.
    // This hack is introduced in order to avoid storing temporary storage
    // in an external container, which may cause memory leaking problems.
    // This pointer is disengaged for regular raw_value_view instances.
    // Data is stored in a shared pointer for two reasons:
    // - pointers are cheap to copy
    // - it makes the view keep its semantics - it's safe to copy a view multiple times
    //   and all copies still refer to the same underlying data.
    lw_shared_ptr<managed_bytes> _temporary_storage = nullptr;

    raw_value_view(null_value&& data)
        : _data{std::move(data)}
    {}
    raw_value_view(unset_value&& data)
        : _data{std::move(data)}
    {}
    raw_value_view(fragmented_temporary_buffer::view data)
        : _data{data}
    {}
    raw_value_view(managed_bytes_view data)
            : _data{data}
    {}
    // This constructor is only used by make_temporary() and it acquires ownership
    // of the given buffer. The view created that way refers to its own temporary storage.
    explicit raw_value_view(managed_bytes&& temporary_storage);
public:
    static raw_value_view make_null() {
        return raw_value_view{std::move(null_value{})};
    }
    static raw_value_view make_unset_value() {
        return raw_value_view{std::move(unset_value{})};
    }
    static raw_value_view make_value(fragmented_temporary_buffer::view view) {
        return raw_value_view{view};
    }
    static raw_value_view make_value(managed_bytes_view view) {
        return raw_value_view{view};
    }
    static raw_value_view make_temporary(raw_value&& value);
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
#if 0
    const fragmented_temporary_buffer::view* operator->() const {
        return &std::get<fragmented_temporary_buffer::view>(_data);
    }
    const fragmented_temporary_buffer::view& operator*() const {
        return std::get<fragmented_temporary_buffer::view>(_data);
    }
#endif
    template <std::invocable<bytes_view> Func>
    std::invoke_result_t<Func, bytes_view> with_linearized(Func&& func) const {
        if (auto pdata = std::get_if<fragmented_temporary_buffer::view>(&_data)) {
            return ::with_linearized(*pdata, std::forward<Func>(func));
        } else {
            return std::get<managed_bytes_view>(_data).with_linearized(std::forward<Func>(func));
        }
    }
    template <std::invocable<bytes_view> Func>
    void with_fragmented(Func&& func) const {
        if (auto pdata = std::get_if<fragmented_temporary_buffer::view>(&_data)) {
            for (bytes_view frag : *pdata) {
                func(frag);
            }
        } else {
            for (bytes_view frag : std::get<managed_bytes_view>(_data).as_fragment_range()) {
                func(frag);
            }
        }
    }
    template <typename Func>
    decltype(auto) with_fragment_range(Func&& func) const {
        if (auto pdata = std::get_if<fragmented_temporary_buffer::view>(&_data)) {
            return func(*pdata);
        } else {
            return func(std::get<managed_bytes_view>(_data).as_fragment_range());
        }
    }
    size_t size_bytes() const {
        if (auto pdata = std::get_if<fragmented_temporary_buffer::view>(&_data)) {
            return pdata->size_bytes();
        }
        if (auto pdata = std::get_if<managed_bytes_view>(&_data)) {
            return pdata->size();
        }
        return 0;
    }

#if 0
    bool operator==(const raw_value_view& other) const {
        if (is_value() && other.is_value()) {
            if (auto p1 = std::get_if<fragmented_temporary_buffer::view>(&_data)) {
                if (auto p2 = std::get_if<fragmented_temporary_buffer::view>(&other._data)) {
                } else {
                    auto& p2 = std::get<managed_bytes_view>(other._data);
                }
            }
            if (auto pdata = std::get_if<managed_bytes_view>(&_data)) {
                return pdata->size();
            }
        } else {
            return _data.index() == other._data.index();
        }
    }
    bool operator!=(const raw_value_view& other) const {
        return !(*this == other);
    }
#endif

    friend std::ostream& operator<<(std::ostream& os, const raw_value_view& value);
};

/// \brief Raw CQL protocol value.
///
/// The `raw_value` type represents an uninterpreted value from the CQL wire
/// protocol. A raw value can hold either a null value, an unset value, or a byte
/// blob that represents the value.
class raw_value {
    std::variant<managed_bytes, null_value, unset_value> _data;

    raw_value(null_value&& data)
        : _data{std::move(data)}
    {}
    raw_value(unset_value&& data)
        : _data{std::move(data)}
    {}
    raw_value(bytes&& data)
        : _data{std::move(data)}
    {}
    raw_value(const bytes& data)
        : _data{data}
    {}
    raw_value(managed_bytes&& data)
            : _data{std::move(data)}
    {}
    raw_value(const managed_bytes& data)
            : _data{data}
    {}
public:
    static raw_value make_null() {
        return raw_value{std::move(null_value{})};
    }
    static raw_value make_unset_value() {
        return raw_value{std::move(unset_value{})};
    }
    static raw_value make_value(const raw_value_view& view);
    static raw_value make_value(bytes&& bytes) {
        return raw_value{std::move(bytes)};
    }
    static raw_value make_value(const bytes& bytes) {
        return raw_value{bytes};
    }
    static raw_value make_value(const bytes_opt& bytes) {
        if (bytes) {
            return make_value(*bytes);
        }
        return make_null();
    }
    static raw_value make_value(managed_bytes&& bytes) {
        return raw_value{std::move(bytes)};
    }
    static raw_value make_value(const managed_bytes& bytes) {
        return raw_value{bytes};
    }
    static raw_value make_value(const std::optional<managed_bytes>& bytes) {
        if (bytes) {
            return make_value(*bytes);
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
        return std::holds_alternative<managed_bytes>(_data);
    }
    std::optional<managed_bytes> data() const {
        if (auto pdata = std::get_if<managed_bytes>(&_data)) {
            return *pdata;
        }
        return {};
    }
    explicit operator bool() const {
        return is_value();
    }
    const managed_bytes* operator->() const {
        return &std::get<managed_bytes>(_data);
    }
    const managed_bytes& operator*() const {
        return std::get<managed_bytes>(_data);
    }
    managed_bytes&& extract_value() && {
        auto b = std::get_if<managed_bytes>(&_data);
        assert(b);
        return std::move(*b);
    }
    raw_value_view to_view() const;
};

}

inline bytes to_bytes(const cql3::raw_value_view& view)
{
    return view.with_linearized([] (bytes_view bv) {
        return bytes(bv);
    });
}

inline bytes_opt to_bytes_opt(const cql3::raw_value_view& view) {
    if (view.is_value()) {
        return view.with_linearized([] (bytes_view bv) {
            return bytes_opt(bytes(bv));
        });
    }
    return bytes_opt();
}

inline bytes_opt to_bytes_opt(const cql3::raw_value& value) {
    return to_bytes_opt(value.to_view());
}
