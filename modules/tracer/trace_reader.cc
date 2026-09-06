// The record loop, interpreted rather than generated. See trace_reader.h.
//
// It is the same loop the viewer's decoder plugin has, one indirection looser:
// where the plugin has a switch from id to a reader compiled for that
// tracepoint's fields, this has a `for` over the fields the table gave. Keep
// them the same shape -- the streams merged by timestamp, the prologue read by
// position, an address placed against the mappings as of the record being read
// -- because a difference between them is a difference in what the format is.

#include "trace_reader.h"

#include <algorithm>
#include <cstring>
#include <format>
#include <map>
#include <stdexcept>
#include <utility>

namespace trace_test {
namespace {

// sizeof(tracer::tracepoint_entry): an address that is `n` strides past an
// object's table is its entry `n`. Asserted on the producer's side in tracer.h
// and repeated in tracepoint_table.cc, which is where a change to it has to be
// noticed.
constexpr std::size_t entry_stride = 64;

constexpr std::uint32_t trace_magic = 0x32435254;  // "TRC2"
constexpr std::uint8_t metadata_level = 2;         // tracer::event_level::metadata

// The tracer's own tracepoints, which have to be read before any record can be
// placed: until the load events have been read, no address is in any object.
constexpr std::string_view clock_sync_name = "clock_sync";
constexpr std::string_view objects_loaded_name = "trace_objects_loaded";
constexpr std::string_view object_loaded_name = "trace_object_loaded";
constexpr std::string_view object_unloaded_name = "trace_object_unloaded";

bool is_signed(std::string_view type) {
    return type == "i8" || type == "i16" || type == "i32" || type == "i64";
}

// How a value prints. The rendering the generated decoder's to_string() had,
// kept to the byte: a byte span as lowercase hex, a pointer as 0x..., a bool as
// a word, a location as file:line:column.
std::string render(const field& f) {
    if (f.type == "bool") return f.number != 0 ? "true" : "false";
    if (f.type == "ptr") return std::format("{:#x}", f.number);
    if (f.type == "str") return f.bytes;
    if (f.type == "bytes") {
        std::string out;
        for (const char c : f.bytes) {
            out += std::format("{:02x}", static_cast<unsigned char>(c));
        }
        return out;
    }
    if (f.type == "srcloc") return f.location.to_string();
    if (is_signed(f.type)) return std::format("{}", f.signed_number);
    return std::format("{}", f.number);
}

// One field off the wire, by the token its signature gave it. A type this has
// no reader for is not a field that can be skipped: how long it is is part of
// what it is, so the record -- and every record after it in the stream -- stops
// here.
field read_field(const tracepoints::field& declared, const std::byte*& p, const std::byte* end,
                 const trace::detail::locator& where) {
    field out;
    out.name = declared.name;
    out.type = declared.type;
    const std::string_view type = declared.type;

    const auto unsigned_of = [&](std::uint64_t value) { out.number = value; };
    const auto signed_of = [&](std::int64_t value) {
        out.signed_number = value;
        out.number = static_cast<std::uint64_t>(value);
    };

    if (type == "u8") {
        unsigned_of(trace::detail::read_unaligned<std::uint8_t>(p, end));
    } else if (type == "u16") {
        unsigned_of(trace::detail::read_unaligned<std::uint16_t>(p, end));
    } else if (type == "u32") {
        unsigned_of(trace::detail::read_unaligned<std::uint32_t>(p, end));
    } else if (type == "u64") {
        unsigned_of(trace::detail::read_unaligned<std::uint64_t>(p, end));
    } else if (type == "i8") {
        signed_of(trace::detail::read_unaligned<std::int8_t>(p, end));
    } else if (type == "i16") {
        signed_of(trace::detail::read_unaligned<std::int16_t>(p, end));
    } else if (type == "i32") {
        signed_of(trace::detail::read_unaligned<std::int32_t>(p, end));
    } else if (type == "i64") {
        signed_of(trace::detail::read_unaligned<std::int64_t>(p, end));
    } else if (type == "bool") {
        unsigned_of(trace::detail::read_unaligned<bool>(p, end) ? 1 : 0);
    } else if (type == "ptr") {
        unsigned_of(trace::detail::read_unaligned<std::uintptr_t>(p, end));
    } else if (type == "str") {
        const std::string_view text = trace::detail::read_str(p, end);
        out.bytes.assign(text);
    } else if (type == "bytes") {
        const std::span<const std::byte> raw = trace::detail::read_bytes(p, end);
        out.bytes.assign(reinterpret_cast<const char*>(raw.data()), raw.size());
    } else if (type == "srcloc") {
        // Only the address is on the wire. Which object it is in, and so what
        // file it names, is a question about the mappings as of this record --
        // which is why it is resolved here and not by the caller.
        out.location.address = trace::detail::read_unaligned<std::uint64_t>(p, end);
        where.resolve(out.location);
    } else {
        throw std::runtime_error(std::format(
            "parameter \"{}\" has type \"{}\", which this reader has none for", declared.name,
            declared.type));
    }

    out.text = render(out);
    return out;
}

std::uint64_t read_timestamp(std::uint8_t encoding, const std::byte*& p, const std::byte* end,
                             std::uint64_t last) {
    switch (encoding) {
        case 1: return trace::detail::read_timestamp_sync(p, end, last);
        case 2: return trace::detail::read_timestamp_none(p, end, last);
        default: return trace::detail::read_timestamp_delta(p, end, last);
    }
}

}  // namespace

const field* event::find(std::string_view name) const {
    for (const field& f : fields) {
        if (f.name == name) {
            return &f;
        }
    }
    return nullptr;
}

std::string event::to_string() const {
    std::string out = name;
    out += '{';
    for (std::size_t i = 0; i < fields.size(); ++i) {
        if (i != 0) {
            out += ", ";
        }
        out += fields[i].name;
        out += '=';
        out += fields[i].text;
    }
    out += '}';
    return out;
}

trace_reader::trace_reader(const std::string& dso_root) : root_(dso_root) {
    tables_ = tracepoints::read_tables(root_, notes_);
    if (tables_.empty()) {
        throw std::runtime_error(
            std::format("no object under {}/.build-id has a tracepoint table", root_));
    }
}

std::vector<event> trace_reader::decode(std::span<const std::byte> trace) const {
    trace::dso_directory dsos(root_);
    return decode(trace, dsos);
}

std::vector<event> trace_reader::decode(std::span<const std::byte> trace,
                                        trace::dso_directory& dsos) const {
    // The ids, in the order they run: every object's entries in table order,
    // objects in the order read_tables() sorted them. An address is turned into
    // one of these by the object it was mapped into; a static id says which it
    // is on its own.
    std::vector<const tracepoints::entry*> by_id;
    std::vector<trace::object_descriptor> descriptors;
    std::map<std::uint64_t, std::size_t> static_ids;
    for (const tracepoints::object& object : tables_) {
        descriptors.push_back({object.build_id, static_cast<std::uint32_t>(by_id.size()),
                               static_cast<std::uint32_t>(object.entries.size())});
        for (const tracepoints::entry& entry : object.entries) {
            if (entry.static_id != 0) {
                static_ids.try_emplace(entry.static_id, by_id.size());
            }
            by_id.push_back(&entry);
        }
    }

    // The tracepoint of a given name, for the three the prologue is read as.
    // They are read by their position in the metadata stream rather than by
    // their ids, because until they have been read no address means anything --
    // so what each position *is* has to be known here rather than found there.
    const auto tracepoint_named = [&by_id](std::string_view name) {
        for (const tracepoints::entry* entry : by_id) {
            if (entry->name == name) {
                return entry;
            }
        }
        throw std::runtime_error(
            std::format("no object here has the tracer's own \"{}\" tracepoint: these tables "
                        "are not the ones a trace was written from",
                        name));
    };
    const tracepoints::entry* const sync_entry = tracepoint_named(clock_sync_name);
    const tracepoints::entry* const count_entry = tracepoint_named(objects_loaded_name);
    const tracepoints::entry* const load_entry = tracepoint_named(object_loaded_name);

    const std::byte* p = trace.data();
    const std::byte* const end = p + trace.size();
    if (const auto magic = trace::detail::read_unaligned<std::uint32_t>(p, end);
        magic != trace_magic) {
        throw std::runtime_error(std::format("not a trace: magic {:#x}", magic));
    }

    // One level's records. The metadata stream goes first because the merge
    // below breaks ties towards the earlier stream: a load event stamped with
    // the same timestamp as the first record from the object it loads has to be
    // read before it.
    struct stream {
        const std::byte* p;
        const std::byte* end;
        std::uint64_t last_timestamp = 0;
    };
    std::vector<stream> streams;
    bool have_metadata = false;
    while (p < end) {
        const auto level = trace::detail::read_unaligned<std::uint8_t>(p, end);
        const auto length = trace::detail::read_unaligned<std::uint64_t>(p, end);
        trace::detail::require(p, end, length);
        if (level == metadata_level) {
            if (have_metadata) {
                throw std::runtime_error("a trace has one metadata stream; this one has two");
            }
            have_metadata = true;
            streams.insert(streams.begin(), {p, p + length});
        } else {
            streams.push_back({p, p + length});
        }
        p += length;
    }
    if (!have_metadata) {
        throw std::runtime_error(
            "a trace with no metadata stream: nothing in it says where its objects were mapped");
    }

    trace::detail::locator where(dsos);
    std::vector<trace::detail::mapping>& mappings = where.mappings;

    // A record's body, read against the tracepoint its id named.
    const auto read_body = [&where](const tracepoints::entry& entry, const std::byte*& q,
                                    const std::byte* q_end) {
        std::vector<field> fields;
        fields.reserve(entry.fields.size());
        for (const tracepoints::field& declared : entry.fields) {
            fields.push_back(read_field(declared, q, q_end, where));
        }
        return fields;
    };

    const auto value_of = [](const std::vector<field>& fields, std::string_view name) {
        for (const field& f : fields) {
            if (f.name == name) {
                return f.number;
            }
        }
        throw std::runtime_error(
            std::format("the tracer's own tracepoint has no \"{}\" parameter", name));
    };
    const auto text_of = [](const std::vector<field>& fields, std::string_view name) {
        for (const field& f : fields) {
            if (f.name == name) {
                return f.bytes;
            }
        }
        throw std::runtime_error(
            std::format("the tracer's own tracepoint has no \"{}\" parameter", name));
    };

    const auto load = [&](const std::vector<field>& fields) {
        const std::string build_id = text_of(fields, "build_id");
        const trace::object_descriptor* found = nullptr;
        for (const trace::object_descriptor& object : descriptors) {
            if (object.build_id == build_id) {
                found = &object;
            }
        }
        // The build ID is copied out of the trace and kept, because a mapping
        // outlives the record that made it and the string_view forms here point
        // into the buffer either way.
        mappings.push_back({value_of(fields, "table_address"), value_of(fields, "base_address"),
                            value_of(fields, "mapping_size"), found, {}});
        mappings.back().build_id = found != nullptr ? found->build_id : std::string_view{};
        std::sort(mappings.begin(), mappings.end(),
                  [](const trace::detail::mapping& a, const trace::detail::mapping& b) {
                      return a.table < b.table;
                  });
    };
    const auto unload = [&](const std::vector<field>& fields) {
        const std::uint64_t base = value_of(fields, "base_address");
        for (auto it = mappings.begin(); it != mappings.end(); ++it) {
            if (it->base == base) {
                mappings.erase(it);
                return;
            }
        }
        throw std::runtime_error("an object was unloaded without having been loaded");
    };

    // Which tracepoint a record names. `no_decoder_id` for one that cannot be
    // placed, without a word about why: this is asked twice, once on the peek
    // that orders the streams -- where an address whose object has not been
    // loaded yet is not an error -- and once on the record being read, where
    // refuse() says what is wrong.
    const auto placed_id = [&](const trace::detail::record_id& which) -> std::uint32_t {
        if (which.is_static) {
            const auto found = static_ids.find(which.value);
            return found == static_ids.end() ? trace::no_decoder_id
                                             : static_cast<std::uint32_t>(found->second);
        }
        const std::uint64_t address = which.value;
        const auto above = std::upper_bound(
            mappings.begin(), mappings.end(), address,
            [](std::uint64_t value, const trace::detail::mapping& m) { return value < m.table; });
        if (above == mappings.begin() || (above - 1)->object == nullptr) {
            return trace::no_decoder_id;
        }
        const trace::detail::mapping& from = *(above - 1);
        const std::uint64_t offset = address - from.table;
        if (offset % entry_stride != 0 || offset / entry_stride >= from.object->count) {
            return trace::no_decoder_id;
        }
        return static_cast<std::uint32_t>(from.object->first_id + offset / entry_stride);
    };

    const auto refuse = [&mappings](const trace::detail::record_id& which, std::uint64_t after) {
        if (which.is_static) {
            throw std::runtime_error(std::format(
                "static tracepoint id {}, in a record after {}, is not one of these objects'",
                which.value, after));
        }
        const std::uint64_t address = which.value;
        const auto above = std::upper_bound(
            mappings.begin(), mappings.end(), address,
            [](std::uint64_t value, const trace::detail::mapping& m) { return value < m.table; });
        if (above == mappings.begin()) {
            throw std::runtime_error(
                std::format("tracepoint address {:#x} is below every object loaded after {}",
                            address, after));
        }
        const trace::detail::mapping& from = *(above - 1);
        if (from.object == nullptr) {
            throw std::runtime_error(std::format(
                "tracepoint address {:#x} belongs to an object whose tracepoint table this "
                "decoder has not got -- is it missing from the dso directory?",
                address));
        }
        throw std::runtime_error(
            std::format("tracepoint address {:#x} is not an entry of the object that was at "
                        "{:#x} after {}",
                        address, from.table, after));
    };

    // The prologue: a clock sync, a count, and that many load events. Read by
    // that invariant, and timed by the encodings those three tracepoints
    // declare -- which is as much as can be known about a record whose id
    // cannot yet be placed. See "the metadata stream" in tracer.h.
    {
        stream& meta = streams.front();
        trace::detail::read_record_id(meta.p, meta.end);
        meta.last_timestamp = read_timestamp(sync_entry->timestamps, meta.p, meta.end, 0);
        (void)read_body(*sync_entry, meta.p, meta.end);

        trace::detail::read_record_id(meta.p, meta.end);
        meta.last_timestamp =
            read_timestamp(count_entry->timestamps, meta.p, meta.end, meta.last_timestamp);
        const std::uint64_t count = value_of(read_body(*count_entry, meta.p, meta.end), "count");
        for (std::uint64_t i = 0; i < count; ++i) {
            trace::detail::read_record_id(meta.p, meta.end);
            meta.last_timestamp =
                read_timestamp(load_entry->timestamps, meta.p, meta.end, meta.last_timestamp);
            load(read_body(*load_entry, meta.p, meta.end));
        }
    }

    std::vector<event> events;
    while (true) {
        // The earliest record still unread, over every stream. Reading its
        // timestamp means placing it first -- how the front of a body is timed
        // is a fact about the tracepoint -- and both are peeked without
        // advancing the stream.
        stream* next = nullptr;
        std::uint64_t earliest = 0;
        for (stream& candidate : streams) {
            if (candidate.p == candidate.end) {
                continue;
            }
            const std::byte* peek = candidate.p;
            const trace::detail::record_id which =
                trace::detail::read_record_id(peek, candidate.end);
            const std::uint32_t id = placed_id(which);
            const std::uint8_t encoding = id == trace::no_decoder_id ? 0 : by_id[id]->timestamps;
            const std::uint64_t at =
                read_timestamp(encoding, peek, candidate.end, candidate.last_timestamp);
            if (next == nullptr || at < earliest) {
                next = &candidate;
                earliest = at;
            }
        }
        if (next == nullptr) {
            break;
        }

        const std::byte*& q = next->p;
        const std::byte* const q_end = next->end;
        const trace::detail::record_id which = trace::detail::read_record_id(q, q_end);

        // Placed before the timestamp is read rather than after, because how
        // many bytes of the body are the timestamp -- and whether there are any
        // -- is what the id says.
        const std::uint32_t id = placed_id(which);
        if (id == trace::no_decoder_id) {
            refuse(which, next->last_timestamp);
        }
        const tracepoints::entry& entry = *by_id[id];
        const std::uint64_t timestamp =
            read_timestamp(entry.timestamps, q, q_end, next->last_timestamp);
        // Left where it was by a record that carries no timestamp of its own,
        // which is what makes the next record in this buffer a delta from the
        // same place.
        next->last_timestamp = timestamp;

        std::vector<field> fields = read_body(entry, q, q_end);

        // The metadata stream is the frame the rest of the trace is read in
        // rather than events of the program's own: its records are consumed
        // here and not delivered.
        if (entry.name == object_loaded_name) {
            load(fields);
            continue;
        }
        if (entry.name == object_unloaded_name) {
            unload(fields);
            continue;
        }
        if (entry.name == objects_loaded_name) {
            continue;
        }

        event out;
        out.name = entry.name;
        out.file = entry.file;
        out.function = entry.function;
        out.line = entry.line;
        out.has_timestamp = entry.timestamps != 2;
        out.timestamp = timestamp;
        out.fields = std::move(fields);
        events.push_back(std::move(out));
    }
    return events;
}

}  // namespace trace_test
