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
    int64_t to_logical_fixme() const noexcept {
        return _value;
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
    int64_t to_logical_fixme() const noexcept {
        return _value;
    }
};

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
