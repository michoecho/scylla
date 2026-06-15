/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <compare>
#include <cstdint>
#include <fmt/core.h>

namespace sstables {

class sstable_datafile_offset;

class sstable_datafile_position {
    int64_t _value;
    explicit sstable_datafile_position(int64_t v) noexcept : _value(v) {}
public:
    sstable_datafile_position() = default;
    sstable_datafile_position(const sstable_datafile_position&) = default;
    sstable_datafile_position& operator=(const sstable_datafile_position&) = default;

    auto operator<=>(const sstable_datafile_position&) const = default;
    bool operator==(const sstable_datafile_position&) const = default;

    static sstable_datafile_position from_logical_fixme(int64_t v) noexcept {
        return sstable_datafile_position(v);
    }
    static sstable_datafile_position from_logical_approved(int64_t v) noexcept {
        return sstable_datafile_position(v);
    }
    int64_t to_logical_fixme() const noexcept {
        return _value;
    }
    int64_t to_logical_approved() const noexcept {
        return _value;
    }

    friend sstable_datafile_position operator+(sstable_datafile_position pos, sstable_datafile_offset off) noexcept;
};

// disk_read_range describes a byte ranges covering part of an sstable
// row that we need to read from disk. Usually this is the whole byte
// range covering a single sstable row, but in very large rows we might
// want to only read a subset of the atoms which we know contains the
// columns we are looking for.
struct disk_read_range {
    // TODO: this should become a vector of ranges
    sstable_datafile_position start;
    sstable_datafile_position end;

    disk_read_range(sstable_datafile_position start, sstable_datafile_position end) :
        start(start), end(end) { }
    explicit operator bool() const {
        return start != end;
    }
};

class sstable_datafile_offset {
    int64_t _value;
    explicit sstable_datafile_offset(int64_t v) noexcept : _value(v) {}
public:
    sstable_datafile_offset() = default;
    sstable_datafile_offset(const sstable_datafile_offset&) = default;
    sstable_datafile_offset& operator=(const sstable_datafile_offset&) = default;

    auto operator<=>(const sstable_datafile_offset&) const = default;
    bool operator==(const sstable_datafile_offset&) const = default;

    static sstable_datafile_offset from_logical_fixme(int64_t v) noexcept {
        return sstable_datafile_offset(v);
    }
    static sstable_datafile_offset from_logical_approved(int64_t v) noexcept {
        return sstable_datafile_offset(v);
    }
    int64_t to_logical_fixme() const noexcept {
        return _value;
    }

    friend sstable_datafile_position operator+(sstable_datafile_position pos, sstable_datafile_offset off) noexcept;
};

inline sstable_datafile_position operator+(sstable_datafile_position pos, sstable_datafile_offset off) noexcept {
    return sstable_datafile_position(pos._value + off._value);
}

} // namespace sstables

template <>
struct fmt::formatter<sstables::sstable_datafile_position> : fmt::formatter<int64_t> {
    auto format(const sstables::sstable_datafile_position& p, fmt::format_context& ctx) const {
        return fmt::formatter<int64_t>::format(p.to_logical_fixme(), ctx);
    }
};

template <>
struct fmt::formatter<sstables::sstable_datafile_offset> : fmt::formatter<int64_t> {
    auto format(const sstables::sstable_datafile_offset& o, fmt::format_context& ctx) const {
        return fmt::formatter<int64_t>::format(o.to_logical_fixme(), ctx);
    }
};
