/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/noncopyable_function.hh>
#include "seastarx.hh"
#include "sstables/sstable_datafile_position.hh"

namespace sstables {

/// \brief Interface for streams used for reading sstable data files.
///
/// The class exists as a distinct type so that sstable-specific behavior
/// (e.g. integrity checking, instrumentation) can be added without changing
/// callers. A default implementation that wraps a \ref seastar::input_stream
/// is provided; alternative implementations can be supplied via the
/// \c impl-taking constructor.
class sstable_datafile_input_stream {
public:
    using char_type = char;
    using tmp_buf = temporary_buffer<char>;
    using consumer_fn = noncopyable_function<future<consumption_result<char>>(temporary_buffer<char>)>;

    /// \brief Abstract implementation backing a \ref sstable_datafile_input_stream.
    class impl {
    public:
        virtual ~impl() = default;
        virtual future<tmp_buf> read_exactly(size_t n) noexcept = 0;
        virtual future<> consume(consumer_fn consumer) noexcept = 0;
        virtual bool eof() const noexcept = 0;
        virtual future<tmp_buf> read() noexcept = 0;
        virtual future<> close() noexcept = 0;
        virtual future<> skip(uint64_t n) noexcept = 0;
        virtual future<> skip_to(sstable_datafile_position target, sstable_datafile_position current) noexcept = 0;
        virtual data_source detach() && = 0;
        virtual sstable_datafile_position compute_relative_position(sstable_datafile_position pos, ssize_t offset) = 0;
        virtual int64_t subtract_positions(sstable_datafile_position b, sstable_datafile_position a) = 0;
    };

private:
    std::unique_ptr<impl> _impl;

public:
    /// \brief Constructs a stream backed by the default implementation,
    /// which wraps the supplied \c input_stream.
    explicit sstable_datafile_input_stream(input_stream<char> stream);

    /// \brief Constructs a stream that takes ownership of a custom
    /// implementation.
    explicit sstable_datafile_input_stream(std::unique_ptr<impl> i) noexcept
        : _impl(std::move(i)) {}

    sstable_datafile_input_stream(sstable_datafile_input_stream&&) = default;

    /// \brief Reads exactly \c n bytes from the stream, or fewer if end of
    /// stream is reached.
    future<tmp_buf> read_exactly(size_t n) noexcept {
        return _impl->read_exactly(n);
    }

    /// \brief Consumes the stream using the supplied consumer.
    template <typename Consumer>
    future<> consume(Consumer&& c) noexcept(std::is_nothrow_move_constructible_v<Consumer>) {
        return _impl->consume(consumer_fn(std::forward<Consumer>(c)));
    }

    /// \brief Consumes the stream using the supplied consumer reference.
    template <typename Consumer>
    future<> consume(Consumer& c) noexcept {
        return _impl->consume(consumer_fn(std::ref(c)));
    }

    /// \brief Returns true if the end-of-file flag is set on the stream.
    bool eof() const noexcept {
        return _impl->eof();
    }

    /// \brief Returns some data from the stream, or an empty buffer on end
    /// of stream.
    future<tmp_buf> read() noexcept {
        return _impl->read();
    }

    /// \brief Closes the stream and waits for any background operations to
    /// complete.
    future<> close() noexcept {
        return _impl->close();
    }

    /// \brief Ignores the next \c n bytes from the stream.
    future<> skip(uint64_t n) noexcept {
        return _impl->skip(n);
    }

    /// \brief Advances the stream to the absolute position \c target.
    ///
    /// \c current must be the position of the next byte the stream would
    /// produce (i.e. the position the caller has reached so far). It is used
    /// by implementations that cannot determine their own position to compute
    /// the relative distance to skip; implementations that track their position
    /// (e.g. cursor-backed streams) may ignore it and seek to \c target
    /// directly.
    future<> skip_to(sstable_datafile_position target, sstable_datafile_position current) noexcept {
        return _impl->skip_to(target, current);
    }

    sstable_datafile_position compute_relative_position(sstable_datafile_position pos, ssize_t offset) {
        return _impl->compute_relative_position(pos, offset);
    }

    int64_t subtract_positions(sstable_datafile_position b, sstable_datafile_position a) {
        return _impl->subtract_positions(b, a);
    }

    /// \brief Detaches the underlying \c data_source from the wrapped
    /// implementation, leaving this object in a moved-from state.
    data_source detach() && {
        return std::move(*_impl).detach();
    }
};

} // namespace sstables
