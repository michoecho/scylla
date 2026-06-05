/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>
#include "seastarx.hh"

namespace sstables {

/// \brief Thin wrapper around \ref seastar::input_stream<char> used for
/// reading sstable data files.
///
/// All methods delegate to the wrapped \ref input_stream. The class exists
/// as a distinct type so that future sstable-specific behavior (e.g.
/// integrity checking, instrumentation) can be added without changing
/// callers.
class sstable_datafile_input_stream {
    input_stream<char> _stream;
public:
    using char_type = char;
    using tmp_buf = temporary_buffer<char>;

    explicit sstable_datafile_input_stream(input_stream<char> stream) noexcept
        : _stream(std::move(stream)) {}

    sstable_datafile_input_stream(sstable_datafile_input_stream&&) = default;

    /// \brief Reads exactly \c n bytes from the stream, or fewer if end of
    /// stream is reached.
    future<tmp_buf> read_exactly(size_t n) noexcept {
        return _stream.read_exactly(n);
    }

    /// \brief Consumes the stream using the supplied consumer.
    template <typename Consumer>
    future<> consume(Consumer&& c) noexcept(std::is_nothrow_move_constructible_v<Consumer>) {
        return _stream.consume(std::forward<Consumer>(c));
    }

    /// \brief Consumes the stream using the supplied consumer reference.
    template <typename Consumer>
    future<> consume(Consumer& c) noexcept(std::is_nothrow_move_constructible_v<Consumer>) {
        return _stream.consume(c);
    }

    /// \brief Returns true if the end-of-file flag is set on the stream.
    bool eof() const noexcept {
        return _stream.eof();
    }

    /// \brief Returns some data from the stream, or an empty buffer on end
    /// of stream.
    future<tmp_buf> read() noexcept {
        return _stream.read();
    }

    /// \brief Closes the stream and waits for any background operations to
    /// complete.
    future<> close() noexcept {
        return _stream.close();
    }

    /// \brief Ignores the next \c n bytes from the stream.
    future<> skip(uint64_t n) noexcept {
        return _stream.skip(n);
    }

    /// \brief Detaches the underlying \c data_source from the wrapped
    /// \c input_stream, leaving this object in a moved-from state.
    data_source detach() && {
        return std::move(_stream).detach();
    }
};

} // namespace sstables
