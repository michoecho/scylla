// The trace wire format, as a reader.
//
// A trace is records; a record is an id, a timestamp and the packed bytes of a
// tracepoint's arguments. What that id means -- which tracepoint, with which
// parameters, in which order -- is not in the trace: it is in the `tracepoints`
// section of the object that wrote it. So this header is deliberately only half
// a decoder. It knows *how* a record is laid out and nothing about what any
// particular one contains, and the other half -- the tables, read out of the
// objects by tracepoint_table.h and turned into a switch by decoder_plugin.cc --
// is generated per set of objects and compiled at startup.
//
// Two programs include this. The viewer, to read tracepoint tables out of the
// ELF objects behind a snapshot; and each generated plugin, which is this
// header plus a switch over the ids those tables gave. That is why it is
// standalone -- no fmt, no viewer headers, nothing but the standard library and
// Linux -- because the plugin is compiled at runtime by whatever compiler is on
// PATH, against nothing but a copy of this file.
//
// Everything a decoded record points at points into the trace buffer it was
// read from, and lives exactly as long as it does. The one exception is a
// source location, which is read out of an object file and copied, because the
// mapping it came from is not the record's to keep.
//
// Derived from the fixed part of the header modules/tracer used to generate --
// which was the second description of this format, and is gone: this is the
// only one now. See modules/tracer/include/tracer/tracer.h for the writer's
// side of every encoding here, and modules/tracer/trace_reader.h for the tests
// that write with one and read with the other.

#pragma once


#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <format>
#include <fstream>
#include <iterator>
#include <map>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

// mmap and friends. The objects a decode reads are big -- half a gigabyte for a
// Scylla build with its debug info -- and it touches a few kilobytes of each, so
// they are mapped rather than read. Linux only, like the tracer itself.
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace trace {

// Which tracepoint a record came from, as its entry in an object's table said:
// everything about a record that is a fact about the call site rather than
// about the moment it fired.
//
// Views rather than pointers: a plugin's table of these is a constexpr array
// over string literals either way, and a pointer would cost a `strlen` per
// string per record on the way into the viewer's event_meta. That is 5 ms of a
// 137 ms decode in the default build -- small, but it buys nothing.
//
// `has_timestamp` is false for a tracepoint declared with TRACEPOINT_UNTIMED(),
// whose records carry no time of their own: such a record is handed the moment
// of the record before it in its buffer, which is a real answer to "when" but
// not the record's own, and a consumer that needs one of its own has to make it
// up. See timestamp_encoding in tracer.h.
struct meta_info {
    std::string_view name;
    std::string_view file;
    int line = 0;
    std::string_view function;
    bool has_timestamp = true;
};

// Where a record's *address* comes from: one object's slice of the ids.
//
// A record names its tracepoint by the address of an entry, which is only
// meaningful against the object the entry is in -- so an address is turned into
// an id by finding the object it was mapped into and dividing the offset by the
// entry stride. `first_id` is the id of the object's entry 0.
struct object_descriptor {
    std::string_view build_id;
    std::uint32_t first_id;
    std::uint32_t count;
};

// An id nothing could be placed as: a static id no object claims, or an address
// in no object's table. Not an error on its own -- see the peek in a generated
// decode loop -- so it has a value rather than an exception.
inline constexpr std::uint32_t no_decoder_id = 0xffffffffU;

namespace detail {

// Every read is bounded by the end of the trace. A record is not
// self-delimiting, so a truncated one cannot be recognised as short until a
// field runs off the end -- and a field that ran off the end must be an
// exception rather than a view into whatever follows the buffer.
inline void require(const std::byte* p, const std::byte* end, std::size_t n) {
    if (static_cast<std::size_t>(end - p) < n) {
        throw std::runtime_error(
            std::format("truncated trace: {} bytes left, {} wanted", end - p, n));
    }
}

template <typename To>
    requires std::is_trivially_copyable_v<To>
To read_unaligned(const std::byte*& p, const std::byte* end) {
    require(p, end, sizeof(To));
    To dst;
    std::memcpy(&dst, p, sizeof(To));
    p += sizeof(To);
    return dst;
}

// A record's first field: which tracepoint it is. Either the address of an
// entry, which is eight-byte aligned and so ends in three zero bits, or a vint
// of a static id doubled and made odd, which cannot. See "static ids" in
// tracer.h.
struct record_id {
    bool is_static;
    std::uint64_t value;  // the entry address, or the static id itself
};

inline std::uint64_t read_int(const std::byte*& p, const std::byte* end) {
    // The length is a run of one bits at the bottom of the first byte, and the
    // value sits above it, little-endian. See write_int() in tracer.h.
    require(p, end, 1);
    const auto first = std::to_integer<std::uint8_t>(*p);
    const std::size_t size = static_cast<std::size_t>(std::countr_one(first)) + 1;
    if (size > 8) {
        // Nothing writes one: a value of more than 56 bits would need a tag
        // byte of its own, and what goes through here is a record's age within
        // its buffer. See write_int_sized() in tracer.h.
        throw std::runtime_error("a vint of more than eight bytes");
    }
    require(p, end, size);
    std::uint64_t word = 0;
    std::memcpy(&word, p, size);
    p += size;
    return word >> size;
}

inline record_id read_record_id(const std::byte*& p, const std::byte* end) {
    require(p, end, 1);
    if ((std::to_integer<std::uint8_t>(*p) & 0b111) == 0) {
        return {false, read_unaligned<std::uint64_t>(p, end)};
    }
    // Undo the doubling the writer did to keep those bits out of the way. A
    // short id is a short read: an address is eight bytes here and a static id
    // is as few as one, which is the whole point of it.
    return {true, (read_int(p, end) - 1) / 2};
}

// --- the timestamp at the head of a body --------------------------------------
//
// A record is an id and then a body, and the body opens with the timestamp --
// in whichever of these forms the tracepoint's own declaration chose. Which one
// that is comes from the id, so these are reached through read_timestamp()
// below rather than called from the record loop directly.
//
// `last` is the timestamp of the record before this one in the same buffer,
// which is what a delta is measured from. Each returns the timestamp of the
// record and leaves `p` on its first argument.

inline std::uint64_t read_timestamp_delta(const std::byte*& p, const std::byte* end,
                                          std::uint64_t last) {
    return last + read_int(p, end);
}

// A clock sync, which opens a buffer and so has no record before it to count
// from: its delta is measured from the tick count in its own first parameter.
// That parameter is left on the wire -- it is an argument like any other, and
// the reader below reads it again -- so only the delta is consumed here.
inline std::uint64_t read_timestamp_sync(const std::byte*& p, const std::byte* end,
                                         [[maybe_unused]] std::uint64_t last) {
    const std::uint64_t delta = read_int(p, end);
    const std::byte* base = p;
    return read_unaligned<std::uint64_t>(base, end) + delta;
}

// A tracepoint that writes no timestamp at all. The record happened when the
// one before it did, as far as anything reading this can tell, and nothing is
// consumed: the body is its arguments and nothing else.
inline std::uint64_t read_timestamp_none([[maybe_unused]] const std::byte*& p,
                                         [[maybe_unused]] const std::byte* end,
                                         std::uint64_t last) {
    return last;
}

// Length-prefixed runs. Both views point into the trace buffer rather than
// copying out of it, which is what keeps decoding allocation-free -- and what
// makes them valid only as long as that buffer is.
inline std::span<const std::byte> read_bytes(const std::byte*& p, const std::byte* end) {
    const auto len = read_unaligned<std::uint16_t>(p, end);
    require(p, end, len);
    const std::span<const std::byte> out(p, len);
    p += len;
    return out;
}

inline std::string_view read_str(const std::byte*& p, const std::byte* end) {
    const std::span<const std::byte> raw = read_bytes(p, end);
    return {reinterpret_cast<const char*>(raw.data()), raw.size()};
}

// How to_string() renders one field. Free functions rather than a formatter
// specialisation, so that a caller printing a field itself can reach for the
// same rendering without adopting it for every std::span in its program.
}  // namespace detail

// --- resolving a source location ----------------------------------------------
//
// A srcloc::location on the wire is one address, and nothing else: the address
// of a constant the compiler laid down in the object that captured it. Reading
// it back is three steps.
//
//   1. Which object was it in? The metadata stream says where each object was
//      mapped -- its base and how far it reached -- as of the moment the record
//      was written, so this is the same "read the mappings at that timestamp"
//      lookup a tracepoint address goes through, against a different range.
//   2. Subtract the base. What is left is a virtual address in the object's own
//      link-time layout, which is a fact about the *file* rather than the run.
//   3. Open the file and read it. The object is found by build ID, in the
//      directory laid out below; the location is the four fields of a
//      std::source_location, and the two of them that are pointers are more
//      virtual addresses in the same object, read the same way.
//
// A location that cannot be taken through all three -- an object the directory
// does not have, a run recorded before it was mapped -- comes out unresolved
// rather than wrong, with the address it was recorded as. See resolved below.

// One decoded source location. Owning strings rather than views into the object
// file, so that an event outlives the dso_directory it was resolved against;
// everything else a decoded event holds points into the trace, and this is the
// one field that would otherwise point somewhere with a shorter life.
struct source_location {
    std::string file;
    std::string function;
    std::uint32_t line = 0;
    std::uint32_t column = 0;

    std::uint64_t address = 0;  // as recorded, and all there is if !resolved
    std::string object;         // build ID of the object it was found in
    bool resolved = false;

    [[nodiscard]] std::string to_string() const {
        if (resolved) {
            return std::format("{}:{}:{}", file, line, column);
        }
        if (address == 0) {
            return "<none>";  // srcloc::location::none(), recorded as it was
        }
        return std::format("<unresolved {:#x}>", address);
    }
};

namespace detail {

// Where a link-time virtual address lands in the object's file, or nullptr if
// it lands nowhere that has `size` bytes behind it.
//
// The ELF header and program headers are read by offset rather than through
// <elf.h>, so that a decoder built anywhere can read a trace from a Linux
// x86-64 program -- which is the only kind there is. Anything that is not a
// little-endian 64-bit ELF is refused rather than misread.
[[nodiscard]] inline const std::byte* at_vaddr(std::span<const std::byte> image,
                                               std::uint64_t vaddr, std::size_t size) {
    constexpr std::size_t e_phoff_at = 0x20;
    constexpr std::size_t e_phentsize_at = 0x36;
    constexpr std::size_t e_phnum_at = 0x38;
    constexpr std::uint32_t pt_load = 1;

    const auto word = [image](std::size_t at, std::size_t width) -> std::uint64_t {
        std::uint64_t value = 0;
        std::memcpy(&value, image.data() + at, width);
        return value;
    };
    const auto fits = [image](std::size_t at, std::size_t width) {
        return at + width <= image.size();
    };

    if (!fits(0, 0x40) || std::memcmp(image.data(), "\x7f" "ELF\x02\x01", 6) != 0) {
        return nullptr;
    }
    const std::uint64_t phoff = word(e_phoff_at, 8);
    const std::uint64_t phentsize = word(e_phentsize_at, 2);
    const std::uint64_t phnum = word(e_phnum_at, 2);

    for (std::uint64_t i = 0; i < phnum; ++i) {
        const auto at = static_cast<std::size_t>(phoff + i * phentsize);
        if (!fits(at, 56)) {
            return nullptr;
        }
        if (word(at, 4) != pt_load) {
            continue;
        }
        const std::uint64_t p_offset = word(at + 8, 8);
        const std::uint64_t p_vaddr = word(at + 16, 8);
        const std::uint64_t p_filesz = word(at + 32, 8);
        if (vaddr < p_vaddr || vaddr - p_vaddr >= p_filesz) {
            continue;
        }
        // A .bss address is in the segment's memory size but not its file size,
        // and has nothing behind it to read.
        const std::uint64_t offset = p_offset + (vaddr - p_vaddr);
        if (!fits(static_cast<std::size_t>(offset), size)) {
            return nullptr;
        }
        return image.data() + offset;
    }
    return nullptr;
}

// One object file, mapped rather than read into memory.
//
// These are big -- a Scylla binary with its debug info is half a gigabyte -- and
// a decode touches a few kilobytes of each: the program headers, the dynamic
// relocations, and whatever strings the locations point at. Reading the whole
// file in to look at that copies the entire object for nothing, and pins it in
// the heap for as long as the decoder lives. Mapping hands out the same
// std::span and lets the kernel fault in only the pages actually touched.
class mapped_file {
public:
    mapped_file() = default;

    explicit mapped_file(const std::string& path) {
        const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) {
            return;
        }
        struct ::stat info {};
        if (::fstat(fd, &info) == 0 && info.st_size > 0) {
            void* const p = ::mmap(nullptr, static_cast<std::size_t>(info.st_size), PROT_READ,
                                   MAP_PRIVATE, fd, 0);
            if (p != MAP_FAILED) {
                data_ = static_cast<const std::byte*>(p);
                size_ = static_cast<std::size_t>(info.st_size);
            }
        }
        // The mapping holds its own reference to the file, so the descriptor is
        // not wanted past here -- and a decoder that mapped a dozen objects
        // would otherwise sit on a dozen descriptors for its whole life.
        ::close(fd);
    }

    ~mapped_file() {
        if (data_ != nullptr) {
            ::munmap(const_cast<std::byte*>(data_), size_);
        }
    }

    mapped_file(mapped_file&& other) noexcept : data_(other.data_), size_(other.size_) {
        other.data_ = nullptr;
        other.size_ = 0;
    }

    mapped_file& operator=(mapped_file&& other) noexcept {
        if (this != &other) {
            if (data_ != nullptr) {
                ::munmap(const_cast<std::byte*>(data_), size_);
            }
            data_ = other.data_;
            size_ = other.size_;
            other.data_ = nullptr;
            other.size_ = 0;
        }
        return *this;
    }

    mapped_file(const mapped_file&) = delete;
    mapped_file& operator=(const mapped_file&) = delete;

    [[nodiscard]] bool ok() const { return data_ != nullptr; }
    [[nodiscard]] std::span<const std::byte> bytes() const { return {data_, size_}; }

private:
    const std::byte* data_ = nullptr;
    std::size_t size_ = 0;
};

// The relative relocations of one object, as (place, value) pairs sorted by
// place -- both link-time virtual addresses.
//
// A pointer stored inside a shared object is not in the object's file. x86-64
// uses RELA, whose addend lives in the relocation entry rather than at the place
// it patches, so the file holds a zero there and the value is only in
// .rela.dyn; the loader writes it as the object is mapped. The two pointers of a
// source location -- its file and its function -- are exactly that, so a decoder
// reading them straight out of the image would get nothing but a zero, and
// resolve every location in every shared library to the same wrong thing.
//
// Two kinds are collected, because a tracepoint table has both. A pointer to
// something in the same translation unit is an R_X86_64_RELATIVE, whose value is
// the addend and nothing else. A pointer to a string in an *inline* function --
// a tracepoint written in a header, whose name and signature live in a comdat
// the linker folded -- is an R_X86_64_64 against the symbol that survived, whose
// value is that symbol's address plus the addend: the same arithmetic the loader
// does, over the symbol table that is in the file. A relocation against an
// undefined symbol has no value here and is left out; nothing a table points at
// is one.
//
// DT_RELR needs nothing here: it packs offsets and leaves the value in place, so
// the file already holds it.
//
// An object with no dynamic segment -- a non-PIE executable is the usual one --
// has no such relocations and needs none: its pointers are absolute and already
// written down.
[[nodiscard]] inline std::vector<std::pair<std::uint64_t, std::uint64_t>> pointer_relocations(
    std::span<const std::byte> image) {
    constexpr std::size_t e_phoff_at = 0x20;
    constexpr std::size_t e_phentsize_at = 0x36;
    constexpr std::size_t e_phnum_at = 0x38;
    constexpr std::uint32_t pt_dynamic = 2;
    constexpr std::uint64_t dt_null = 0;
    constexpr std::uint64_t dt_rela = 7;
    constexpr std::uint64_t dt_relasz = 8;
    constexpr std::uint64_t dt_relaent = 9;
    constexpr std::uint64_t dt_symtab = 6;
    constexpr std::uint64_t dt_syment = 11;
    constexpr std::uint32_t r_x86_64_64 = 1;
    constexpr std::uint32_t r_x86_64_relative = 8;

    std::vector<std::pair<std::uint64_t, std::uint64_t>> out;
    const auto word = [image](std::size_t at, std::size_t width) -> std::uint64_t {
        std::uint64_t value = 0;
        std::memcpy(&value, image.data() + at, width);
        return value;
    };
    if (image.size() < 0x40 || std::memcmp(image.data(), "\x7f" "ELF\x02\x01", 6) != 0) {
        return out;
    }
    const std::uint64_t phoff = word(e_phoff_at, 8);
    const std::uint64_t phentsize = word(e_phentsize_at, 2);
    const std::uint64_t phnum = word(e_phnum_at, 2);

    std::uint64_t dynamic_at = 0;
    std::uint64_t dynamic_size = 0;
    for (std::uint64_t i = 0; i < phnum; ++i) {
        const auto at = static_cast<std::size_t>(phoff + i * phentsize);
        if (at + 56 > image.size()) {
            return out;
        }
        if (word(at, 4) == pt_dynamic) {
            dynamic_at = word(at + 16, 8);   // p_vaddr
            dynamic_size = word(at + 40, 8); // p_memsz
        }
    }
    const std::byte* const dynamic =
        at_vaddr(image, dynamic_at, static_cast<std::size_t>(dynamic_size));
    if (dynamic == nullptr) {
        return out;
    }

    std::uint64_t rela = 0;
    std::uint64_t relasz = 0;
    std::uint64_t relaent = 24;
    std::uint64_t symtab = 0;
    std::uint64_t syment = 24;
    for (std::uint64_t at = 0; at + 16 <= dynamic_size; at += 16) {
        std::uint64_t tag = 0;
        std::uint64_t value = 0;
        std::memcpy(&tag, dynamic + at, sizeof(tag));
        std::memcpy(&value, dynamic + at + 8, sizeof(value));
        if (tag == dt_null) {
            break;
        }
        if (tag == dt_rela) rela = value;
        if (tag == dt_relasz) relasz = value;
        if (tag == dt_relaent && value != 0) relaent = value;
        if (tag == dt_symtab) symtab = value;
        if (tag == dt_syment && value != 0) syment = value;
    }
    if (rela == 0 || relasz == 0) {
        return out;
    }
    const std::byte* const entries = at_vaddr(image, rela, static_cast<std::size_t>(relasz));
    if (entries == nullptr) {
        return out;
    }
    // An Elf64_Sym is 24 bytes with st_value eight in, which is all that is
    // wanted of one.
    const auto symbol_value = [&](std::uint64_t index) -> std::uint64_t {
        if (symtab == 0) {
            return 0;
        }
        const std::byte* const symbol = at_vaddr(image, symtab + index * syment, 16);
        if (symbol == nullptr) {
            return 0;
        }
        std::uint64_t value = 0;
        std::memcpy(&value, symbol + 8, sizeof(value));
        return value;
    };

    for (std::uint64_t at = 0; at + 24 <= relasz; at += relaent) {
        std::uint64_t place = 0;
        std::uint64_t info = 0;
        std::uint64_t addend = 0;
        std::memcpy(&place, entries + at, sizeof(place));
        std::memcpy(&info, entries + at + 8, sizeof(info));
        std::memcpy(&addend, entries + at + 16, sizeof(addend));
        const auto type = static_cast<std::uint32_t>(info);
        if (type == r_x86_64_relative) {
            out.emplace_back(place, addend);
        } else if (type == r_x86_64_64) {
            if (const std::uint64_t value = symbol_value(info >> 32); value != 0) {
                out.emplace_back(place, value + addend);
            }
        }
    }
    std::sort(out.begin(), out.end());
    return out;
}

// The value a pointer field holds once the loader has been through it: what is
// written at `place` in the file, or the relocation's addend where the file
// holds nothing.
[[nodiscard]] inline std::uint64_t relocated(
    const std::vector<std::pair<std::uint64_t, std::uint64_t>>& relocations, std::uint64_t place,
    std::uint64_t in_file) {
    if (in_file != 0) {
        return in_file;
    }
    const auto found = std::lower_bound(relocations.begin(), relocations.end(),
                                        std::pair<std::uint64_t, std::uint64_t>{place, 0});
    return found != relocations.end() && found->first == place ? found->second : in_file;
}

// A NUL-terminated string at a virtual address, bounded by the end of the file:
// an object naming a location it does not hold is a corrupt object, not a read
// off the end of one.
[[nodiscard]] inline std::string string_at_vaddr(std::span<const std::byte> image,
                                                 std::uint64_t vaddr) {
    const std::byte* const p = at_vaddr(image, vaddr, 1);
    if (p == nullptr) {
        return {};
    }
    const auto* text = reinterpret_cast<const char*>(p);
    const std::size_t room = image.size() - static_cast<std::size_t>(p - image.data());
    const void* const nul = std::memchr(text, '\0', room);
    return nul == nullptr ? std::string{} : std::string(text);
}


// One section of an object, by name: where it was laid out at link time, and
// how big it is. Empty if the object has not got it.
//
// Section headers rather than program headers, which is the one thing here that
// wants them -- an object stripped of its section headers has no `tracepoints`
// to find, and says so by coming back empty. `objcopy --strip-debug` keeps
// them; `--strip-all` does not, and neither does a linker told to strip.
struct section {
    std::uint64_t vaddr = 0;
    std::uint64_t size = 0;
};

[[nodiscard]] inline section section_by_name(std::span<const std::byte> image,
                                             std::string_view name) {
    constexpr std::size_t e_shoff_at = 0x28;
    constexpr std::size_t e_shentsize_at = 0x3a;
    constexpr std::size_t e_shnum_at = 0x3c;
    constexpr std::size_t e_shstrndx_at = 0x3e;

    const auto word = [image](std::size_t at, std::size_t width) -> std::uint64_t {
        std::uint64_t value = 0;
        std::memcpy(&value, image.data() + at, width);
        return value;
    };
    const auto fits = [image](std::size_t at, std::size_t width) {
        return at + width <= image.size();
    };
    if (!fits(0, 0x40) || std::memcmp(image.data(), "\x7f" "ELF\x02\x01", 6) != 0) {
        return {};
    }
    const std::uint64_t shoff = word(e_shoff_at, 8);
    const std::uint64_t shentsize = word(e_shentsize_at, 2);
    const std::uint64_t shnum = word(e_shnum_at, 2);
    const std::uint64_t shstrndx = word(e_shstrndx_at, 2);
    if (shoff == 0 || shnum == 0 || shstrndx >= shnum) {
        return {};
    }

    // The section header string table, which every section's name is an offset
    // into. Read by file offset rather than by virtual address: it is not
    // loaded, so it has no address at all.
    const auto strings_at = static_cast<std::size_t>(shoff + shstrndx * shentsize);
    if (!fits(strings_at, 64)) {
        return {};
    }
    const std::uint64_t strings_offset = word(strings_at + 24, 8);
    const std::uint64_t strings_size = word(strings_at + 32, 8);
    if (!fits(static_cast<std::size_t>(strings_offset), static_cast<std::size_t>(strings_size))) {
        return {};
    }

    for (std::uint64_t i = 0; i < shnum; ++i) {
        const auto at = static_cast<std::size_t>(shoff + i * shentsize);
        if (!fits(at, 64)) {
            return {};
        }
        const std::uint64_t name_at = word(at, 4);
        if (name_at >= strings_size) {
            continue;
        }
        const auto* const text =
            reinterpret_cast<const char*>(image.data() + strings_offset + name_at);
        const std::size_t room = static_cast<std::size_t>(strings_size - name_at);
        const void* const nul = std::memchr(text, '\0', room);
        if (nul == nullptr) {
            continue;  // a name running off the end of the table: not this one
        }
        if (static_cast<std::size_t>(static_cast<const char*>(nul) - text) == name.size() &&
            std::memcmp(text, name.data(), name.size()) == 0) {
            return {word(at + 16, 8), word(at + 32, 8)};
        }
    }
    return {};
}

}  // namespace detail


// The objects a trace needs to be decoded against, by build ID.
//
//     <root>/.build-id/<first two hex digits>/<the rest>[.debug]
//
// which is the layout gdb and llvm-cov already expect from a
// --debug-file-directory, and what tracer::write_dso_directory() produces. The
// whole file is read rather than mapped: a decode reads a handful of scattered
// words out of each object, and a lifetime that ends when this object does is
// worth more here than the pages saved.
//
// One of these is shared by decodes running at once -- the viewer decodes a
// snapshot's files a thread apiece -- so the two lazy caches are locked. Only
// the lookup is: nothing here is ever erased or overwritten, so the bytes and
// the relocations a caller was handed stay put while other threads add their
// own objects beside them.
class dso_directory {
public:
    explicit dso_directory(std::string root) : root_(std::move(root)) {}

    // Where a decoder looks when its caller says nothing: $TRACE_DSO_DIR, or the
    // working directory.
    dso_directory() {
        const char* const from_env = std::getenv("TRACE_DSO_DIR");
        root_ = from_env != nullptr ? from_env : ".";
    }

    // The object's bytes, or an empty span if the directory does not have it.
    // Misses are remembered too: a trace holds many records from one object, and
    // a missing object should be one failed open rather than thousands.
    [[nodiscard]] std::span<const std::byte> object(const std::string& build_id) {
        const std::lock_guard<std::mutex> held(mutex_);
        const auto found = files_.find(build_id);
        if (found != files_.end()) {
            return found->second.bytes();
        }
        detail::mapped_file image;
        if (build_id.size() >= 3) {
            const std::string stem =
                root_ + "/.build-id/" + build_id.substr(0, 2) + "/" + build_id.substr(2);
            // With the suffix first, because that is what a debuginfo directory
            // holds; without it for a directory of plain binaries.
            for (const std::string& path : {stem + ".debug", stem}) {
                detail::mapped_file candidate(path);
                if (candidate.ok()) {
                    image = std::move(candidate);
                    break;
                }
            }
        }
        return files_.emplace(build_id, std::move(image)).first->second.bytes();
    }

    // Where the object's file is, or an empty string if the directory does not
    // have it. For handing to something that reads objects itself -- addr2line
    // over a stack of frames, say -- rather than for reading here.
    [[nodiscard]] std::string path(const std::string& build_id) const {
        if (build_id.size() < 3) {
            return {};
        }
        const std::string stem =
            root_ + "/.build-id/" + build_id.substr(0, 2) + "/" + build_id.substr(2);
        // Same two candidates, in the same order, as object() above.
        for (const std::string& candidate : {stem + ".debug", stem}) {
            if (std::ifstream(candidate, std::ios::binary)) {
                return candidate;
            }
        }
        return {};
    }

    // The object's relative relocations, read once. A location's file and
    // function pointers are not in the file of a shared object -- see
    // detail::pointer_relocations() -- and a trace holds a location per record,
    // so this is built on first use and kept.
    [[nodiscard]] const std::vector<std::pair<std::uint64_t, std::uint64_t>>& relocations(
        const std::string& build_id) {
        // Outside the lock, because object() takes it as well and reading the
        // file is the expensive half of this either way.
        const std::span<const std::byte> image = object(build_id);
        const std::lock_guard<std::mutex> held(mutex_);
        const auto found = relocations_.find(build_id);
        if (found != relocations_.end()) {
            return found->second;
        }
        return relocations_.emplace(build_id, detail::pointer_relocations(image)).first->second;
    }

private:
    std::string root_;
    std::mutex mutex_;  // files_ and relocations_, which are built on demand
    std::map<std::string, detail::mapped_file, std::less<>> files_;
    std::map<std::string, std::vector<std::pair<std::uint64_t, std::uint64_t>>, std::less<>>
        relocations_;
};

// The directory a decode uses when its caller names none. One per process, so
// that a program decoding several traces reads each object once; a caller that
// wants a directory of its own passes it to decode() instead.
[[nodiscard]] inline dso_directory& shared_dso_directory() {
    static dso_directory directory;
    return directory;
}
namespace detail {


// One object, for as long as it is mapped. `object` is null for a build ID this
// decoder was not generated from: the mapping is still kept, because it is what
// stops that object's records being attributed to the object below it -- they
// are refused by name instead.
struct mapping {
    std::uint64_t table;  // where the object's tracepoint table was mapped
    std::uint64_t base;   // and where the object itself begins
    std::uint64_t size;   // how far past the base it reaches
    const object_descriptor* object;
    std::string_view build_id;
    // What resolving a location in this object needs, looked up in the
    // directory when the mapping is made rather than per record: a trace holds
    // a location per record and a handful of load events. Empty and null for
    // an object the directory has not got. See locator::add.
    std::span<const std::byte> image;
    const std::vector<std::pair<std::uint64_t, std::uint64_t>>* fixups = nullptr;
};

// The mappings a record is read against, and the objects a location is read out
// of. One of these lives for the length of a decode; the mappings change as its
// metadata stream is consumed, which is what makes a record decode against the
// process as it was at the record's own timestamp.
class locator {
public:
    explicit locator(dso_directory& dsos) : dsos_(&dsos) {}

    std::vector<mapping> mappings;  // sorted by table address

    // An object was mapped: the one place a mapping is made, because it is
    // where the object's bytes and relocations are fetched out of the
    // directory. Kept sorted so that "the object a tracepoint address is in"
    // is a binary search for the greatest table address not above it.
    void add(std::uint64_t table, std::uint64_t base, std::uint64_t size,
             const object_descriptor* object, std::string_view build_id) {
        const std::string id(build_id);
        const std::span<const std::byte> image = dsos_->object(id);
        // A mapping with no bytes behind it is still kept -- it is what stops
        // that object's records being attributed to the object below it -- and
        // there is nothing to relocate against.
        const std::vector<std::pair<std::uint64_t, std::uint64_t>>* fixups =
            image.empty() ? nullptr : &dsos_->relocations(id);
        mappings.push_back({table, base, size, object, build_id, image, fixups});
        std::sort(mappings.begin(), mappings.end(),
                  [](const mapping& a, const mapping& b) { return a.table < b.table; });
    }

    // Fill in a location from the object it points into. Silent about failure
    // by design: a location whose object is not in the directory, or which was
    // recorded before that object was mapped, stays unresolved and keeps the
    // address it came with. That is a decoder missing a file, not a corrupt
    // trace, and it should not stop the other records being read.
    void resolve(source_location& out) const {
        if (out.address == 0) {
            return;  // srcloc::location::none()
        }
        for (const mapping& m : mappings) {
            if (out.address < m.base || out.address - m.base >= m.size) {
                continue;
            }
            out.object = std::string(m.build_id);

            const std::span<const std::byte> image = m.image;
            if (image.empty()) {
                return;  // the object is named, and not in the directory
            }
            // The four fields of a std::source_location, laid out as
            // srcloc::entry describes: two pointers then two 32-bit words. The
            // pointers hold link-time virtual addresses, and are read the same
            // way the entry itself was -- once the relocation that fills them in
            // has been applied, which in a shared object is where they live.
            const std::uint64_t entry_at = out.address - m.base;
            const std::byte* const entry = at_vaddr(image, entry_at, 24);
            if (entry == nullptr) {
                return;
            }
            std::uint64_t file_at = 0;
            std::uint64_t function_at = 0;
            std::memcpy(&file_at, entry, sizeof(file_at));
            std::memcpy(&function_at, entry + 8, sizeof(function_at));
            // In a shared object those two are zero in the file and the value is
            // in .rela.dyn; in a non-PIE executable they are already right.
            if ((file_at == 0 || function_at == 0) && m.fixups != nullptr) {
                file_at = relocated(*m.fixups, entry_at, file_at);
                function_at = relocated(*m.fixups, entry_at + 8, function_at);
            }
            std::memcpy(&out.line, entry + 16, sizeof(out.line));
            std::memcpy(&out.column, entry + 20, sizeof(out.column));
            out.file = string_at_vaddr(image, file_at);
            out.function = string_at_vaddr(image, function_at);
            out.resolved = !out.file.empty();
            return;
        }
    }

private:
    dso_directory* dsos_;
};


}  // namespace detail

}  // namespace trace
