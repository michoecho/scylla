/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <compare>
#include <cstdint>
#include <variant>
#include <fmt/core.h>

#include "utils/assert.hh"

namespace sstables {

class sstable_datafile_offset;

class sstable_datafile_position {
    // A logical position is a simple wrapper over an integer.
    struct logical {
        int64_t value;
        auto operator<=>(const logical&) const = default;
        bool operator==(const logical&) const = default;
    };
    // A physical position is a (chunk_position, chunk_length, offset_within_chunk)
    // triple. It is compared and added like a (chunk_position, offset_within_chunk)
    // tuple (chunk_length does not participate in the ordering).
    struct physical {
        int64_t chunk_position;
        int64_t chunk_length;
        int64_t offset_within_chunk;
    };

    std::variant<logical, physical> _value;

    explicit sstable_datafile_position(logical v) noexcept : _value(v) {}
    explicit sstable_datafile_position(physical v) noexcept : _value(v) {}
public:
    sstable_datafile_position() = default;
    sstable_datafile_position(const sstable_datafile_position&) = default;
    sstable_datafile_position& operator=(const sstable_datafile_position&) = default;

    std::strong_ordering operator<=>(const sstable_datafile_position& other) const noexcept {
        SCYLLA_ASSERT(_value.index() == other._value.index());
        if (auto* a = std::get_if<logical>(&_value)) {
            auto* b = std::get_if<logical>(&other._value);
            return a->value <=> b->value;
        }
        auto* a = std::get_if<physical>(&_value);
        auto* b = std::get_if<physical>(&other._value);
        if (auto cmp = a->chunk_position <=> b->chunk_position; cmp != 0) {
            return cmp;
        }
        return a->offset_within_chunk <=> b->offset_within_chunk;
    }
    bool operator==(const sstable_datafile_position& other) const noexcept {
        return (*this <=> other) == 0;
    }

    static sstable_datafile_position from_logical_fixme(int64_t v) noexcept {
        return sstable_datafile_position(logical{v});
    }
    static sstable_datafile_position from_logical_approved(int64_t v) noexcept {
        return sstable_datafile_position(logical{v});
    }
    static sstable_datafile_position from_physical(int64_t chunk_position, int64_t chunk_length, int64_t offset_within_chunk) noexcept {
        return sstable_datafile_position(physical{chunk_position, chunk_length, offset_within_chunk});
    }
    int64_t to_logical_fixme() const noexcept {
        SCYLLA_ASSERT(std::holds_alternative<logical>(_value));
        return std::get<logical>(_value).value;
    }
    int64_t to_logical_approved() const noexcept {
        SCYLLA_ASSERT(std::holds_alternative<logical>(_value));
        return std::get<logical>(_value).value;
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
    // A logical offset is a simple wrapper over an integer.
    struct logical {
        int64_t value;
        auto operator<=>(const logical&) const = default;
        bool operator==(const logical&) const = default;
    };
    // A physical offset is a (chunk_position, chunk_length, offset_within_chunk)
    // triple, added elementwise to a physical position.
    struct physical {
        int64_t chunk_position;
        int64_t chunk_length;
        int64_t offset_within_chunk;
    };

    std::variant<logical, physical> _value;

    explicit sstable_datafile_offset(logical v) noexcept : _value(v) {}
    explicit sstable_datafile_offset(physical v) noexcept : _value(v) {}
public:
    sstable_datafile_offset() = default;
    sstable_datafile_offset(const sstable_datafile_offset&) = default;
    sstable_datafile_offset& operator=(const sstable_datafile_offset&) = default;

    std::strong_ordering operator<=>(const sstable_datafile_offset& other) const noexcept {
        SCYLLA_ASSERT(_value.index() == other._value.index());
        if (auto* a = std::get_if<logical>(&_value)) {
            auto* b = std::get_if<logical>(&other._value);
            return a->value <=> b->value;
        }
        auto* a = std::get_if<physical>(&_value);
        auto* b = std::get_if<physical>(&other._value);
        if (auto cmp = a->chunk_position <=> b->chunk_position; cmp != 0) {
            return cmp;
        }
        return a->offset_within_chunk <=> b->offset_within_chunk;
    }
    bool operator==(const sstable_datafile_offset& other) const noexcept {
        return (*this <=> other) == 0;
    }

    static sstable_datafile_offset from_logical_fixme(int64_t v) noexcept {
        return sstable_datafile_offset(logical{v});
    }
    static sstable_datafile_offset from_logical_approved(int64_t v) noexcept {
        return sstable_datafile_offset(logical{v});
    }
    static sstable_datafile_offset from_physical(int64_t chunk_position, int64_t chunk_length, int64_t offset_within_chunk) noexcept {
        return sstable_datafile_offset(physical{chunk_position, chunk_length, offset_within_chunk});
    }
    int64_t to_logical_fixme() const noexcept {
        SCYLLA_ASSERT(std::holds_alternative<logical>(_value));
        return std::get<logical>(_value).value;
    }

    friend sstable_datafile_position operator+(sstable_datafile_position pos, sstable_datafile_offset off) noexcept;
};

inline sstable_datafile_position operator+(sstable_datafile_position pos, sstable_datafile_offset off) noexcept {
    SCYLLA_ASSERT(pos._value.index() == off._value.index());
    if (auto* p = std::get_if<sstable_datafile_position::logical>(&pos._value)) {
        auto* o = std::get_if<sstable_datafile_offset::logical>(&off._value);
        return sstable_datafile_position(sstable_datafile_position::logical{p->value + o->value});
    }
    auto* p = std::get_if<sstable_datafile_position::physical>(&pos._value);
    auto* o = std::get_if<sstable_datafile_offset::physical>(&off._value);
    return sstable_datafile_position(sstable_datafile_position::physical{
        p->chunk_position + o->chunk_position,
        p->chunk_length + o->chunk_length,
        p->offset_within_chunk + o->offset_within_chunk,
    });
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
