/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <compare>
#include <cstdint>
#include <optional>
#include <utility>
#include <variant>
#include <fmt/format.h>

#include "utils/assert.hh"

namespace sstables {

class sstable_position_offset;

// A common container for "logical" sstable positions and "physical" sstable
// positions.
// Every sstable uses exactly one of those two kinds, which is decided
// at write time.
//
// All operations involving an sstable and a sstable_position must use the
// right position kind. Doing otherwise is undefined behavior.
// FIXME: this rule is begging for trouble. Maybe there's a better way.
//
// Doing arithmetic between positions of different kinds is likewise forbidden.
class sstable_position {
    // Used with uncompressed sstables,
    // and with compressed sstables which don't support "physical" indexing.
    struct logical {
        int64_t value;
        auto operator<=>(const logical&) const = default;
        bool operator==(const logical&) const = default;
    };
    
    // Used with compressed sstables which support "physical" indexing.
    struct physical {
        // Start position of the chunk within the file.
        int64_t chunk_position;
        // *Upper bound* on the size of the chunk.
        // Used to choose the size of the disk read that loads the chunk.
        // (Physically-indexed sstables store the chunk length in the header of the chunk,
        // so this hint isn't necessary to read the chunk,
        // but it is necessary to read the chunk in one I/O operation).
        //
        // A value of 0 is allowed and means "unknown".
        // In this case readers who want to read the chunk should get
        // the length from its header.
        // (The index isn't allowed to return 0, but readers can use this convention internally).
        //
        // Rationale for "upper bound": the index stores the chunk length with 512B granularity.
        // This shaves off 9 bits without increasing the cost of the disk read.
        //
        // Note: this field doesn't participate in operator<=>.
        int64_t chunk_length_hint;
        // Offset of this position within the chunk after decompression.
        int64_t offset_within_chunk;
    };

    std::variant<logical, physical> _value;

    explicit sstable_position(logical v) noexcept : _value(v) {}
    explicit sstable_position(physical v) noexcept : _value(v) {}
public:
    sstable_position() = default;
    sstable_position(const sstable_position&) = default;
    sstable_position& operator=(const sstable_position&) = default;

    std::strong_ordering operator<=>(const sstable_position& other) const noexcept {
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
    bool operator==(const sstable_position& other) const noexcept {
        return (*this <=> other) == 0;
    }
    static sstable_position from_logical(int64_t v) noexcept {
        return sstable_position(logical{v});
    }
    static sstable_position from_physical(int64_t chunk_position, int64_t chunk_length, int64_t offset_within_chunk) noexcept {
        return sstable_position(physical{chunk_position, chunk_length, offset_within_chunk});
    }

    bool is_physical() const noexcept {
        return std::holds_alternative<physical>(_value);
    }
    const physical& as_physical() const {
        SCYLLA_ASSERT(std::holds_alternative<physical>(_value));
        return std::get<physical>(_value);
    }
    physical& as_physical() {
        return const_cast<physical&>(std::as_const(*this).as_physical());
    }
    int64_t to_logical() const noexcept {
        SCYLLA_ASSERT(std::holds_alternative<logical>(_value));
        return std::get<logical>(_value).value;
    }

    friend sstable_position operator+(sstable_position pos, sstable_position_offset off) noexcept;
    friend sstable_position_offset operator-(sstable_position a, sstable_position b) noexcept;
    friend struct fmt::formatter<sstable_position>;
    friend struct fmt::formatter<std::optional<sstable_position>>;
};

// Basically the same as sstable_position, except it represents a position
// relative to another position (specifically: a position of row relative
// to partition start), and it has different arithmetic operations defined.
class sstable_position_offset {
    struct logical {
        int64_t value;
        auto operator<=>(const logical&) const = default;
        bool operator==(const logical&) const = default;
    };
    struct physical {
        int64_t chunk_position;
        int64_t chunk_length_hint;
        int64_t offset_within_chunk;
    };

    std::variant<logical, physical> _value;

    explicit sstable_position_offset(logical v) noexcept : _value(v) {}
    explicit sstable_position_offset(physical v) noexcept : _value(v) {}
public:
    sstable_position_offset() = default;
    sstable_position_offset(const sstable_position_offset&) = default;
    sstable_position_offset& operator=(const sstable_position_offset&) = default;

    std::strong_ordering operator<=>(const sstable_position_offset& other) const noexcept {
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
    bool operator==(const sstable_position_offset& other) const noexcept {
        return (*this <=> other) == 0;
    }

    static sstable_position_offset from_logical(int64_t v) noexcept {
        return sstable_position_offset(logical{v});
    }
    static sstable_position_offset from_physical(int64_t chunk_position, int64_t chunk_length, int64_t offset_within_chunk) noexcept {
        return sstable_position_offset(physical{chunk_position, chunk_length, offset_within_chunk});
    }
    int64_t to_logical() const noexcept {
        SCYLLA_ASSERT(std::holds_alternative<logical>(_value));
        return std::get<logical>(_value).value;
    }
    bool holds_physical() const noexcept {
        return std::holds_alternative<physical>(_value);
    }

    const physical& as_physical() const {
        SCYLLA_ASSERT(std::holds_alternative<physical>(_value));
        return std::get<physical>(_value);
    }
    physical& as_physical() {
        return const_cast<physical&>(std::as_const(*this).as_physical());
    }

    friend sstable_position operator+(sstable_position pos, sstable_position_offset off) noexcept;
    friend sstable_position_offset operator-(sstable_position a, sstable_position b) noexcept;
    friend struct fmt::formatter<sstable_position_offset>;
};

inline sstable_position operator+(sstable_position pos, sstable_position_offset off) noexcept {
    SCYLLA_ASSERT(pos._value.index() == off._value.index());
    if (auto* p = std::get_if<sstable_position::logical>(&pos._value)) {
        auto* o = std::get_if<sstable_position_offset::logical>(&off._value);
        return sstable_position(sstable_position::logical{p->value + o->value});
    }
    auto* p = std::get_if<sstable_position::physical>(&pos._value);
    auto* o = std::get_if<sstable_position_offset::physical>(&off._value);
    // Note: physical offset's chunk_position is relative to `pos`,
    // but the other two are absolute.
    // This is because making the chunk_position relative saves space,
    // but making the other two relative wouldn't save any space,
    // but it would make them signed integers, making them slightly
    // more annoying to deal with.
    return sstable_position(sstable_position::physical{
        p->chunk_position + o->chunk_position,
        o->chunk_length_hint,
        o->offset_within_chunk,
    });
}

inline sstable_position_offset operator-(sstable_position a, sstable_position b) noexcept {
    SCYLLA_ASSERT(a._value.index() == b._value.index());
    if (auto* la = std::get_if<sstable_position::logical>(&a._value)) {
        auto* lb = std::get_if<sstable_position::logical>(&b._value);
        return sstable_position_offset(sstable_position_offset::logical{la->value - lb->value});
    }
    auto* pa = std::get_if<sstable_position::physical>(&a._value);
    auto* pb = std::get_if<sstable_position::physical>(&b._value);
    return sstable_position_offset(sstable_position_offset::physical{
        pa->chunk_position - pb->chunk_position,
        pa->chunk_length_hint,
        pa->offset_within_chunk,
    });
}

} // namespace sstables

template <>
struct fmt::formatter<sstables::sstable_position> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const sstables::sstable_position& p, fmt::format_context& ctx) const {
        if (auto* l = std::get_if<sstables::sstable_position::logical>(&p._value)) {
            return fmt::format_to(ctx.out(), "{}", l->value);
        }
        auto* ph = std::get_if<sstables::sstable_position::physical>(&p._value);
        return fmt::format_to(ctx.out(), "{{chunk_position={}, chunk_length={}, offset_within_chunk={}}}",
            ph->chunk_position, ph->chunk_length_hint, ph->offset_within_chunk);
    }
};

template <>
struct fmt::formatter<sstables::sstable_position_offset> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const sstables::sstable_position_offset& o, fmt::format_context& ctx) const {
        if (auto* l = std::get_if<sstables::sstable_position_offset::logical>(&o._value)) {
            return fmt::format_to(ctx.out(), "{}", l->value);
        }
        auto* ph = std::get_if<sstables::sstable_position_offset::physical>(&o._value);
        return fmt::format_to(ctx.out(), "{{chunk_position={}, chunk_length={}, offset_within_chunk={}}}",
            ph->chunk_position, ph->chunk_length_hint, ph->offset_within_chunk);
    }
};
