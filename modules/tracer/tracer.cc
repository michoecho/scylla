#include "tracer/tracer.h"

#include <algorithm>
#include <format>
#include <stdexcept>

#include <elf.h>
#include <link.h>

namespace tracer {
namespace {

void set_enabled(const tracepoint_entry& entry, bool enabled) {
    if (enabled) {
        ::static_keys::static_key_enable(&entry.key->key);
    } else {
        ::static_keys::static_key_disable(&entry.key->key);
    }
}
}  // namespace

thread_local trace_buffers* local_tracer = nullptr;

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
    for (int i = 0; i < info->dlpi_phnum; ++i) {
        const ElfW(Phdr)& phdr = info->dlpi_phdr[i];
        if (phdr.p_type != PT_LOAD) {
            continue;
        }
        const ElfW(Addr) begin = info->dlpi_addr + phdr.p_vaddr;
        if (address >= begin && address < begin + phdr.p_memsz) {
            query.found = true;
            query.build_id = build_id_of(*info);
            return 1;  // stop the walk
        }
    }
    return 0;
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
                           {table->start, static_cast<std::size_t>(table->stop - table->start)}});
    }

    // Registry order is load order, which is not a property of the program.
    std::sort(objects.begin(), objects.end(),
              [](const trace_object& a, const trace_object& b) { return a.build_id < b.build_id; });
    return objects;
}

std::vector<std::byte> trace_header() {
    std::vector<std::byte> out;
    auto put = [&out](const auto& value) {
        const auto* bytes = reinterpret_cast<const std::byte*>(&value);
        out.insert(out.end(), bytes, bytes + sizeof(value));
    };

    const std::vector<trace_object> objects = trace_objects();
    put(trace_magic);
    put(static_cast<std::uint32_t>(objects.size()));
    for (const trace_object& object : objects) {
        put(static_cast<std::uint16_t>(object.build_id.size()));
        const auto* bytes = reinterpret_cast<const std::byte*>(object.build_id.data());
        out.insert(out.end(), bytes, bytes + object.build_id.size());
        put(static_cast<std::uint64_t>(object.table_address));
    }
    return out;
}

bool is_enabled(const tracepoint_entry& entry) noexcept {
    return static_key_enabled(entry.key);
}

std::size_t set_tracepoint_enabled(std::string_view name, bool enabled) {
    std::size_t matched = 0;
    for (const tracepoint_entry* entry : tracepoints()) {
        if (entry->name == name) {
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
