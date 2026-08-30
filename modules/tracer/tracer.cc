#include "tracer/tracer.h"

#include <algorithm>
#include <filesystem>
#include <format>
#include <stdexcept>

#include <elf.h>
#include <link.h>

namespace tracer {
namespace {

void set_enabled(const tracepoint_entry& entry, bool enabled) {
    if (entry.key == nullptr) {
        return;  // a metadata tracepoint; see "the metadata stream" in tracer.h
    }
    if (enabled) {
        ::static_keys::static_key_enable(&entry.key->key);
    } else {
        ::static_keys::static_key_disable(&entry.key->key);
    }
}
}  // namespace

thread_local trace_buffers* local_tracer = nullptr;

// The one definition of the registry head; see "the tracepoint registry" in
// tracer.h for why it is here rather than inline in the header.
tracepoint_table*& tracepoint_tables() {
    static tracepoint_table* head = nullptr;
    return head;
}

buffer_group::buffer_group(std::size_t capacity, std::size_t buffer_size)
    : capacity_(capacity), buffer_size_(buffer_size) {
    // Allocate the whole budget up front so that steady-state tracing never
    // calls into the allocator: rotation recycles these rather than growing.
    for (used_ = 0; used_ < capacity_; used_ += buffer_size_) {
        old_.emplace_back();
        old_.back().reserve(buffer_size_);
    }
    current_.resize(buffer_size_);
}

void buffer_group::rotate() {
    // Trim to what was actually written before retiring, so that the slack at
    // the end of the buffer is not part of the record stream.
    current_.resize(cur_pos_);
    used_ += current_.capacity();
    old_.push_back(std::move(current_));
    while (used_ > capacity_) {
        used_ -= old_.front().capacity();
        current_ = std::move(old_.front());
        old_.pop_front();
    }
    current_.resize(buffer_size_);
    cur_pos_ = 0;
}

std::vector<std::byte> buffer_group::collect() const {
    std::size_t total = cur_pos_;
    for (const buffer& b : old_) {
        total += b.size();
    }

    std::vector<std::byte> out;
    out.reserve(total);
    for (const buffer& b : old_) {
        out.insert(out.end(), b.begin(), b.end());
    }
    out.insert(out.end(), current_.begin(), current_.begin() + static_cast<std::ptrdiff_t>(cur_pos_));
    return out;
}

namespace {

// --- naming the object an address is in --------------------------------------
//
// dl_iterate_phdr() rather than dladdr(), because a build ID is a note in the
// object's program headers and dladdr() reports neither those nor the note. The
// walk answers both halves at once: which object a table is in, and what that
// object's build ID is.

struct object_query {
    const void* address;   // in: a byte of the table being placed
    std::string build_id;  // out: hex, empty if the object has no build note
    std::uintptr_t base = 0;      // out: the object's load bias
    std::uint64_t mapping_size = 0;  // out: how far past the base it reaches
    std::string path;      // out: the file it was loaded from, "" for the exe
    bool found = false;    // out: whether any object claimed the address
};

// The build ID note of one loaded object, as lowercase hex.
//
// A PT_NOTE segment holds a run of notes, each an Elf64_Nhdr followed by the
// owner name and the descriptor, both padded to four bytes. The one wanted is
// NT_GNU_BUILD_ID from owner "GNU"; nothing says it is the first, or that there
// is only one segment holding notes, so both are walked.
std::string build_id_of(const dl_phdr_info& info) {
    for (int i = 0; i < info.dlpi_phnum; ++i) {
        const ElfW(Phdr)& phdr = info.dlpi_phdr[i];
        if (phdr.p_type != PT_NOTE) {
            continue;
        }
        const auto* note = reinterpret_cast<const std::byte*>(info.dlpi_addr + phdr.p_vaddr);
        const std::byte* const end = note + phdr.p_memsz;
        while (note + sizeof(ElfW(Nhdr)) <= end) {
            ElfW(Nhdr) header{};
            std::memcpy(&header, note, sizeof(header));
            const std::byte* const name = note + sizeof(header);
            const std::byte* const desc = name + ((header.n_namesz + 3) & ~3U);
            const std::byte* const next = desc + ((header.n_descsz + 3) & ~3U);
            if (next > end || next <= note) {
                break;  // a malformed note, rather than a note that is not ours
            }
            if (header.n_type == NT_GNU_BUILD_ID && header.n_namesz == 4 &&
                std::memcmp(name, "GNU", 4) == 0) {
                std::string hex;
                for (std::size_t at = 0; at < header.n_descsz; ++at) {
                    hex += std::format("{:02x}", std::to_integer<unsigned>(desc[at]));
                }
                return hex;
            }
            note = next;
        }
    }
    return {};
}

int claim_object(dl_phdr_info* info, std::size_t /*size*/, void* data) {
    auto& query = *static_cast<object_query*>(data);
    const auto address = reinterpret_cast<ElfW(Addr)>(query.address);

    // Two things at once: whether this object holds the address, and how far it
    // reaches. The extent is the end of the last PT_LOAD, which has to be
    // gathered over the whole loop rather than taken from the segment that
    // matched -- a location and a tracepoint table are in different segments.
    bool claimed = false;
    ElfW(Addr) extent = 0;
    for (int i = 0; i < info->dlpi_phnum; ++i) {
        const ElfW(Phdr)& phdr = info->dlpi_phdr[i];
        if (phdr.p_type != PT_LOAD) {
            continue;
        }
        const ElfW(Addr) begin = info->dlpi_addr + phdr.p_vaddr;
        extent = std::max(extent, phdr.p_vaddr + phdr.p_memsz);
        if (address >= begin && address < begin + phdr.p_memsz) {
            claimed = true;
        }
    }
    if (!claimed) {
        return 0;
    }
    query.found = true;
    query.build_id = build_id_of(*info);
    query.base = static_cast<std::uintptr_t>(info->dlpi_addr);
    query.mapping_size = static_cast<std::uint64_t>(extent);
    query.path = info->dlpi_name != nullptr ? info->dlpi_name : "";
    return 1;  // stop the walk
}

// The path an object was loaded from. The loader names the main executable with
// the empty string -- it did not open it, the kernel did -- so that one case is
// answered here rather than by every caller.
std::string object_path(const std::string& reported) {
    if (!reported.empty()) {
        return reported;
    }
    std::error_code error;
    const std::filesystem::path self = std::filesystem::read_symlink("/proc/self/exe", error);
    return error ? std::string{} : self.string();
}

}  // namespace

std::vector<const tracepoint_entry*> tracepoints() {
    std::vector<const tracepoint_entry*> all;
    for (const tracepoint_table* table = tracepoint_tables(); table != nullptr;
         table = table->next) {
        for (const tracepoint_entry* entry = table->start; entry < table->stop; ++entry) {
            all.push_back(entry);
        }
    }
    return all;
}

std::vector<trace_object> trace_objects() {
    std::vector<trace_object> objects;
    for (const tracepoint_table* table = tracepoint_tables(); table != nullptr;
         table = table->next) {
        object_query query{.address = table->start};
        ::dl_iterate_phdr(&claim_object, &query);
        if (!query.found) {
            throw std::runtime_error(std::format(
                "tracepoint table at {} belongs to no loaded object", static_cast<const void*>(table->start)));
        }
        if (query.build_id.empty()) {
            throw std::runtime_error(std::format(
                "the object holding the tracepoint table at {} has no GNU build ID; link it "
                "with -Wl,--build-id so that its tracepoints can be named in a trace",
                static_cast<const void*>(table->start)));
        }
        objects.push_back({std::move(query.build_id),
                           reinterpret_cast<std::uintptr_t>(table->start),
                           query.base,
                           query.mapping_size,
                           object_path(query.path),
                           {table->start, static_cast<std::size_t>(table->stop - table->start)}});
    }

    // Registry order is load order, which is not a property of the program.
    std::sort(objects.begin(), objects.end(),
              [](const trace_object& a, const trace_object& b) { return a.build_id < b.build_id; });
    return objects;
}

// --- handing the objects to a decoder ------------------------------------------

void write_dso_directory(const std::string& root) {
    namespace fs = std::filesystem;

    for (const trace_object& object : trace_objects()) {
        if (object.path.empty()) {
            throw std::runtime_error(std::format(
                "object {} was loaded from a path this process cannot name, so it cannot be "
                "collected for a decoder",
                object.build_id));
        }
        // Two digits then the rest, which is the layout every debuginfo
        // consumer already knows; see "handing the objects to a decoder" in
        // tracer.h. A build ID shorter than that is not one.
        if (object.build_id.size() < 3) {
            throw std::runtime_error(
                std::format("object build ID \"{}\" is too short to file", object.build_id));
        }
        const fs::path directory =
            fs::path(root) / ".build-id" / object.build_id.substr(0, 2);

        std::error_code error;
        fs::create_directories(directory, error);
        if (error) {
            throw std::runtime_error(std::format("cannot create {}: {}", directory.string(),
                                                 error.message()));
        }
        // Copied rather than linked: the point of the directory is to outlive
        // the build outputs the objects came from.
        fs::copy_file(object.path, directory / (object.build_id.substr(2) + ".debug"),
                      fs::copy_options::overwrite_existing, error);
        if (error) {
            throw std::runtime_error(std::format("cannot copy {} into {}: {}", object.path,
                                                 directory.string(), error.message()));
        }
    }
}

// --- writing a trace ----------------------------------------------------------

void append_chunk(std::vector<std::byte>& out, event_level level,
                  std::span<const std::byte> records) {
    const auto put = [&out](const auto& value) {
        const auto* bytes = reinterpret_cast<const std::byte*>(&value);
        out.insert(out.end(), bytes, bytes + sizeof(value));
    };
    put(static_cast<std::uint8_t>(level));
    put(static_cast<std::uint64_t>(records.size()));
    out.insert(out.end(), records.begin(), records.end());
}

std::vector<std::byte> collect_trace(const trace_buffers& buffers) {
    std::vector<std::byte> out;
    const auto* magic = reinterpret_cast<const std::byte*>(&trace_magic);
    out.insert(out.end(), magic, magic + sizeof(trace_magic));
    for (std::size_t i = 0; i < trace_buffers::level_count; ++i) {
        const auto level = static_cast<event_level>(i);
        append_chunk(out, level, buffers.group(level).collect());
    }
    return out;
}

bool is_enabled(const tracepoint_entry& entry) noexcept {
    return entry.key != nullptr && static_key_enabled(entry.key);
}

std::size_t set_tracepoint_enabled(std::string_view name, bool enabled) {
    std::size_t matched = 0;
    for (const tracepoint_entry* entry : tracepoints()) {
        if (entry->key != nullptr && entry->name == name) {
            set_enabled(*entry, enabled);
            ++matched;
        }
    }
    return matched;
}

void set_all_tracepoints_enabled(bool enabled) {
    for (const tracepoint_entry* entry : tracepoints()) {
        set_enabled(*entry, enabled);
    }
}

}  // namespace tracer
