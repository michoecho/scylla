#include "tracer/tracer.h"

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <format>
#include <stdexcept>
#include <thread>

#include <ctime>

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

constinit thread_local trace_buffers* local_tracer = nullptr;

namespace {

// Process-wide, and written from a calibration that may run on any thread while
// others are already tracing. Relaxed: a sync record wants a rate, not a
// happens-before edge, and either the old or the new one is a good answer.
std::atomic<std::uint64_t> tsc_rate{default_tsc_ticks_per_second};
std::atomic<bool> clock_sync_on{true};

}  // namespace

std::uint64_t realtime_nanoseconds() noexcept {
    timespec now{};
    ::clock_gettime(CLOCK_REALTIME, &now);
    return static_cast<std::uint64_t>(now.tv_sec) * 1'000'000'000U +
           static_cast<std::uint64_t>(now.tv_nsec);
}

std::uint64_t tsc_ticks_per_second() noexcept {
    return tsc_rate.load(std::memory_order_relaxed);
}

void set_tsc_ticks_per_second(std::uint64_t ticks) noexcept {
    tsc_rate.store(ticks, std::memory_order_relaxed);
}

bool clock_sync_enabled() noexcept { return clock_sync_on.load(std::memory_order_relaxed); }

void set_clock_sync_enabled(bool enabled) noexcept {
    clock_sync_on.store(enabled, std::memory_order_relaxed);
}

std::uint64_t calibrate_tsc(std::chrono::nanoseconds interval) {
    // The wall clock is read *around* each rdtsc rather than beside it, and the
    // midpoints are what the division uses: the two reads bracket the tick
    // count, so the error in placing it is half the cost of a clock_gettime()
    // rather than the whole of it, in either direction.
    const auto sample = [](std::uint64_t& ticks) {
        const std::uint64_t before = realtime_nanoseconds();
        ticks = rdtsc();
        const std::uint64_t after = realtime_nanoseconds();
        return before / 2 + after / 2;
    };

    std::uint64_t first_ticks = 0;
    const std::uint64_t first_ns = sample(first_ticks);
    std::this_thread::sleep_for(interval);
    std::uint64_t second_ticks = 0;
    const std::uint64_t second_ns = sample(second_ticks);

    // A clock that did not move -- a coarse wall clock against too short an
    // interval -- would divide by zero and install a nonsense rate. Keeping
    // what was there is the honest answer: this measured nothing.
    if (second_ns <= first_ns || second_ticks <= first_ticks) {
        return tsc_ticks_per_second();
    }
    const auto measured = static_cast<std::uint64_t>(
        static_cast<double>(second_ticks - first_ticks) /
        static_cast<double>(second_ns - first_ns) * 1e9);
    set_tsc_ticks_per_second(measured);
    return measured;
}

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
        old_.back().bytes.reserve(buffer_size_);
    }
    current_.bytes.resize(buffer_size_);
    current_.activated_ns = TRACER_REALTIME_NS();
}

void buffer_group::rotate() {
    // Trim to what was actually written before retiring, so that the slack at
    // the end of the buffer is not part of the record stream.
    current_.bytes.resize(cur_pos_);
    current_.retired_ns = TRACER_REALTIME_NS();
    used_ += current_.bytes.capacity();
    old_.push_back(std::move(current_));
    while (used_ > capacity_) {
        used_ -= old_.front().bytes.capacity();
        current_ = std::move(old_.front());
        old_.pop_front();
    }
    current_.bytes.resize(buffer_size_);
    // A recycled buffer keeps nothing of the life it had: it is live from now,
    // and not retired at all.
    current_.activated_ns = TRACER_REALTIME_NS();
    current_.retired_ns = 0;
    last_timestamp_ = 0;
    cur_pos_ = 0;
}

std::pair<std::uint64_t, std::uint64_t> buffer_group::time_range() const {
    // The oldest buffer that still holds records. The ones in front of it are
    // the empty ones the constructor put there, which have never been written
    // and whose activation time would place the snapshot before the process
    // started tracing.
    std::uint64_t start = current_.activated_ns;
    for (const buffer& b : old_) {
        if (!b.bytes.empty()) {
            start = b.activated_ns;
            break;
        }
    }
    return {start, TRACER_REALTIME_NS()};
}

std::vector<std::byte> buffer_group::collect() const {
    std::size_t total = cur_pos_;
    for (const buffer& b : old_) {
        total += b.bytes.size();
    }

    std::vector<std::byte> out;
    out.reserve(total);
    for (const buffer& b : old_) {
        out.insert(out.end(), b.bytes.begin(), b.bytes.end());
    }
    out.insert(out.end(), current_.bytes.begin(),
               current_.bytes.begin() + static_cast<std::ptrdiff_t>(cur_pos_));
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

// How far past its base an object reaches: the end of its last PT_LOAD. Gathered
// over every segment rather than taken from one, because the things an address
// may be -- a tracepoint table, a source location -- are in different segments.
std::uint64_t extent_of(const dl_phdr_info& info) {
    ElfW(Addr) extent = 0;
    for (int i = 0; i < info.dlpi_phnum; ++i) {
        const ElfW(Phdr)& phdr = info.dlpi_phdr[i];
        if (phdr.p_type == PT_LOAD) {
            extent = std::max(extent, phdr.p_vaddr + phdr.p_memsz);
        }
    }
    return static_cast<std::uint64_t>(extent);
}

int collect_object(dl_phdr_info* info, std::size_t /*size*/, void* data) {
    auto& all = *static_cast<std::vector<object_query>*>(data);
    all.push_back({nullptr, build_id_of(*info), static_cast<std::uintptr_t>(info->dlpi_addr),
                   extent_of(*info), info->dlpi_name != nullptr ? info->dlpi_name : "", true});
    return 0;  // every object, not the first that matches
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

std::string executable_build_id() {
    // The main executable is the first object dl_iterate_phdr reports, and the
    // one the loader names with the empty string. Taken from the walk rather
    // than from trace_objects(), which sorts by build ID and so loses which one
    // this is.
    std::vector<object_query> loaded;
    ::dl_iterate_phdr(&collect_object, &loaded);
    for (const object_query& object : loaded) {
        if (object.path.empty()) {
            return object.build_id;
        }
    }
    return {};
}

std::vector<trace_object> trace_objects() {
    // Every loaded object, and not only the ones holding tracepoints.
    //
    // A record's address is placed by its object's *table*, so tables were once
    // all this needed to walk. A source location is not: it is an address
    // anywhere in whichever object captured it, and in a program whose
    // tracepoints live in one shared library -- which is how Scylla is put
    // together -- most of them are captured somewhere else entirely. An object
    // this does not name is an address a decoder cannot place at all, so what is
    // described is the whole address space and not the part of it that traces.
    //
    // An object with no build ID is skipped rather than refused, unless it holds
    // a tracepoint table: the vdso is one, and a program does not choose to have
    // it. The refusal below is kept for the case it was written for.
    std::vector<object_query> loaded;
    ::dl_iterate_phdr(&collect_object, &loaded);

    const auto holding = [&loaded](const void* address) -> object_query* {
        const auto at = reinterpret_cast<std::uintptr_t>(address);
        for (object_query& object : loaded) {
            if (at >= object.base && at - object.base < object.mapping_size) {
                return &object;
            }
        }
        return nullptr;
    };

    std::vector<trace_object> objects;
    std::vector<const object_query*> described;
    for (const tracepoint_table* table = tracepoint_tables(); table != nullptr;
         table = table->next) {
        const object_query* const owner = holding(table->start);
        if (owner == nullptr) {
            throw std::runtime_error(std::format(
                "tracepoint table at {} belongs to no loaded object", static_cast<const void*>(table->start)));
        }
        if (owner->build_id.empty()) {
            throw std::runtime_error(std::format(
                "the object holding the tracepoint table at {} has no GNU build ID; link it "
                "with -Wl,--build-id so that its tracepoints can be named in a trace",
                static_cast<const void*>(table->start)));
        }
        objects.push_back({owner->build_id,
                           reinterpret_cast<std::uintptr_t>(table->start),
                           owner->base,
                           owner->mapping_size,
                           object_path(owner->path),
                           {table->start, static_cast<std::size_t>(table->stop - table->start)}});
        described.push_back(owner);
    }

    for (const object_query& object : loaded) {
        if (object.build_id.empty() ||
            std::find(described.begin(), described.end(), &object) != described.end()) {
            continue;
        }
        // No table, so no table address: a decoder places a record by the
        // greatest table address not above it, and zero is below every real one.
        objects.push_back({object.build_id, 0, object.base, object.mapping_size,
                           object_path(object.path), {}});
    }

    // Registry order is load order, which is not a property of the program.
    std::sort(objects.begin(), objects.end(),
              [](const trace_object& a, const trace_object& b) { return a.build_id < b.build_id; });
    return objects;
}

// --- handing the objects to a decoder ------------------------------------------

void write_dso_directory(const std::string& root) {
    namespace fs = std::filesystem;

    // Every object the process has loaded, which since trace_objects() started
    // naming them all is more than the ones that trace: a source location is an
    // address in whichever object captured it, and the copy is what a decoder
    // reads the file and line out of.
    //
    // An object that cannot be collected is fatal only if it holds tracepoints.
    // Without a table it is one object's locations that come out unresolved,
    // which is worth less than refusing to write the directory at all -- and a
    // process has objects it did not choose and cannot open.
    for (const trace_object& object : trace_objects()) {
        const bool required = !object.table.empty();
        if (object.path.empty()) {
            if (!required) {
                continue;
            }
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
            if (!required) {
                continue;  // a library that has since been replaced or removed
            }
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

std::vector<std::byte> collect_trace_level(const trace_buffers& buffers, event_level level) {
    std::vector<std::byte> out;
    const auto* magic = reinterpret_cast<const std::byte*>(&trace_magic);
    out.insert(out.end(), magic, magic + sizeof(trace_magic));
    // The metadata chunk first and always: it is what says where the objects
    // were mapped, and without it the level's records are a heap of addresses.
    // Cheap enough to repeat in every file -- it is one record per loaded
    // object -- and the alternative is a file that only decodes beside another.
    append_chunk(out, event_level::metadata, buffers.group(event_level::metadata).collect());
    if (level != event_level::metadata) {
        append_chunk(out, level, buffers.group(level).collect());
    }
    return out;
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
