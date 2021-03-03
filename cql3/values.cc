/*
 * Copyright (C) 2020 ScyllaDB
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

#include "cql3/values.hh"

namespace cql3 {

std::ostream& operator<<(std::ostream& os, const raw_value_view& value) {
    if (value.is_null()) {
        os << "{ null }";
    } else if (value.is_unset_value()) {
        os << "{ unset }";
    } else {
        os << "{ value: ";
        for (bytes_view frag : fragment_range(*value)) {
            os << frag;
        }
        os << " }";
    }
    return os;
}

#if 0
raw_value raw_value::make_value(const raw_value_view& view) {
    if (view.is_null()) {
        return make_null();
    }
    if (view.is_unset_value()) {
        return make_unset_value();
    }
    return make_value(linearized(*view));
}
#endif

raw_value_view::view raw_value_view::extract_view(const raw_value& v) {
    if (auto p = std::get_if<unset_value>(&v._data)) {
        return view(unset_value());
    } else if (auto p = std::get_if<null_value>(&v._data)) {
        return view(null_value());
    } else if (auto p = std::get_if<bytes>(&v._data)) {
        return view(managed_bytes_view(*p));
    } else {
        return view(std::get<managed_bytes>(v._data));
    }
}

raw_value_view::raw_value_view(raw_value&& value)
    : _temporary_storage(make_lw_shared<raw_value>(std::move(value)))
    , _view(extract_view(*_temporary_storage))
{}

raw_value_view raw_value::to_view() const {
    return raw_value_view(raw_value_view::extract_view(*this));
}

}
