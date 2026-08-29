#include "tracer/tracer.h"

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

std::string_view to_string(log_level level) noexcept {
    switch (level) {
        case log_level::error: return "error";
        case log_level::warn: return "warn";
        case log_level::info: return "info";
        case log_level::debug: return "debug";
        case log_level::trace: return "trace";
    }
    return "?";
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

std::span<const tracepoint_entry> tracepoints() noexcept {
    return {__start_tracepoints,
            static_cast<std::size_t>(__stop_tracepoints - __start_tracepoints)};
}

bool is_enabled(const tracepoint_entry& entry) noexcept {
    return static_key_enabled(entry.key);
}

std::size_t set_tracepoint_enabled(std::string_view name, bool enabled) {
    std::size_t matched = 0;
    for (const tracepoint_entry& entry : tracepoints()) {
        if (entry.name == name) {
            set_enabled(entry, enabled);
            ++matched;
        }
    }
    return matched;
}

void set_all_tracepoints_enabled(bool enabled) {
    for (const tracepoint_entry& entry : tracepoints()) {
        set_enabled(entry, enabled);
    }
}

}  // namespace tracer
