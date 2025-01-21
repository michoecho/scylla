/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils/stream_compressor.hh"
#include <seastar/core/aligned_buffer.hh>

// We need to use experimental features of the zstd library (to allocate compression/decompression context),
// which are available only when the library is linked statically.
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

#include "compress.hh"
#include "exceptions/exceptions.hh"
#include "utils/reusable_buffer.hh"
#include <concepts>

static const sstring COMPRESSION_LEVEL = "compression_level";
const sstring compressor::zstd_class_name = compressor::namespace_prefix + "ZstdCompressor";
static const size_t DCTX_SIZE = ZSTD_estimateDCtxSize();

class zstd_processor : public compressor {
    int _compression_level = 3;
    size_t _cctx_size;

    static auto with_dctx(std::invocable<ZSTD_DCtx*> auto f) {
        // The decompression context has a fixed size of ~128 KiB,
        // so we don't bother ever resizing it the way we do with
        // the compression context.
        static thread_local std::unique_ptr<char[]> buf = std::invoke([&] {
            auto ptr = std::unique_ptr<char[]>(new char[DCTX_SIZE]);
            auto dctx = ZSTD_initStaticDCtx(ptr.get(), DCTX_SIZE);
            if (!dctx) {
                // Barring a bug, this should never happen.
                throw std::runtime_error("Unable to initialize ZSTD decompression context");
            }
            return ptr;
        });
        return f(reinterpret_cast<ZSTD_DCtx*>(buf.get()));
    }

    static auto with_cctx(size_t cctx_size, std::invocable<ZSTD_CCtx*> auto f) {
        // See the comments to reusable_buffer for a rationale of using it for compression.
        static thread_local utils::reusable_buffer<lowres_clock> buf(std::chrono::seconds(600));
        static thread_local size_t last_seen_reallocs = buf.reallocs();
        auto guard = utils::reusable_buffer_guard(buf);
        // Note that the compression context isn't initialized with a particular
        // compression config, but only with a particular size. As long as
        // it is big enough, we can reuse a context initialized by an
        // unrelated instance of zstd_processor without reinitializing it.
        //
        // If the existing context isn't big enough, the reusable buffer will
        // be resized by the next line, and the following `if` will notice that
        // and reinitialize the context.
        auto view = guard.get_temporary_buffer(cctx_size);
        if (last_seen_reallocs != buf.reallocs()) {
            // Either the buffer just grew because we requested a buffer bigger
            // than its last capacity, or it was shrunk some time ago by a timer.
            // Either way, the resize destroyed the contents of the buffer and
            // we have to initialize the context anew.
            auto cctx = ZSTD_initStaticCCtx(view.data(), buf.size());
            if (!cctx) {
                // Barring a bug, this should never happen.
                throw std::runtime_error("Unable to initialize ZSTD compression context");
            }
            last_seen_reallocs = buf.reallocs();
        }
        return f(reinterpret_cast<ZSTD_CCtx*>(view.data()));
    }

public:
    zstd_processor(const opt_getter&);

    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress_max_size(size_t input_len) const override;

    std::set<sstring> option_names() const override;
    std::map<sstring, sstring> options() const override;
};

zstd_processor::zstd_processor(const opt_getter& opts)
    : compressor(compressor::zstd_class_name) {
    auto level = opts(COMPRESSION_LEVEL);
    if (level) {
        try {
            _compression_level = std::stoi(*level);
        } catch (const std::exception& e) {
            throw exceptions::syntax_exception(
                format("Invalid integer value {} for {}", *level, COMPRESSION_LEVEL));
        }

        auto min_level = ZSTD_minCLevel();
        auto max_level = ZSTD_maxCLevel();
        if (min_level > _compression_level || _compression_level > max_level) {
            throw exceptions::configuration_exception(
                format("{} must be between {} and {}, got {}", COMPRESSION_LEVEL, min_level, max_level, _compression_level));
        }
    }

    auto chunk_len_kb = opts(compression_parameters::CHUNK_LENGTH_KB);
    if (!chunk_len_kb) {
        chunk_len_kb = opts(compression_parameters::CHUNK_LENGTH_KB_ERR);
    }
    auto chunk_len = chunk_len_kb
       // This parameter has already been validated.
       ? std::stoi(*chunk_len_kb) * 1024
       : compression_parameters::DEFAULT_CHUNK_LENGTH;

    // We assume that the uncompressed input length is always <= chunk_len.
    auto cparams = ZSTD_getCParams(_compression_level, chunk_len, 0);
    _cctx_size = ZSTD_estimateCCtxSize_usingCParams(cparams);

}

size_t zstd_processor::uncompress(const char* input, size_t input_len, char* output, size_t output_len) const {
    auto ret = with_dctx([&] (ZSTD_DCtx* dctx) {
        return ZSTD_decompressDCtx(dctx, output, output_len, input, input_len);
    });
    if (ZSTD_isError(ret)) {
        throw std::runtime_error( format("ZSTD decompression failure: {}", ZSTD_getErrorName(ret)));
    }
    return ret;
}


size_t zstd_processor::compress(const char* input, size_t input_len, char* output, size_t output_len) const {
    auto ret = with_cctx(_cctx_size, [&] (ZSTD_CCtx* cctx) {
        return ZSTD_compressCCtx(cctx, output, output_len, input, input_len, _compression_level);
    });
    if (ZSTD_isError(ret)) {
        throw std::runtime_error( format("ZSTD compression failure: {}", ZSTD_getErrorName(ret)));
    }
    return ret;
}

size_t zstd_processor::compress_max_size(size_t input_len) const {
    return ZSTD_compressBound(input_len);
}

std::set<sstring> zstd_processor::option_names() const {
    return {COMPRESSION_LEVEL};
}

std::map<sstring, sstring> zstd_processor::options() const {
    return {{COMPRESSION_LEVEL, std::to_string(_compression_level)}};
}

compressor::ptr_type make_zstd_compressor_no_dict(const compressor::opt_getter& o) {
    return seastar::make_shared<zstd_processor>(o);
}

// Throw if ret is an ZSTD error code.
static void check_zstd(size_t ret, const char* text) {
    if (ZSTD_isError(ret)) {
        throw std::runtime_error(fmt::format("{} error: {}", text, ZSTD_getErrorName(ret)));
    }
}

class zstd_processor_with_dict : public compressor {
    class dctx {
        struct deleter {
            void operator()(ZSTD_DCtx* ctx) const noexcept {
                ZSTD_freeDCtx(ctx);
            }
        };
        std::unique_ptr<ZSTD_DCtx, deleter> _ctx;
    public:
        dctx() {
            _ctx.reset(ZSTD_createDCtx());
            if (!_ctx) {
                throw std::bad_alloc();
            }
        }
        ZSTD_DCtx* get() {
            return _ctx.get();
        }
    };

    class cctx {
        struct deleter {
            void operator()(ZSTD_CCtx* ctx) const noexcept {
                ZSTD_freeCCtx(ctx);
            }
        };
        std::unique_ptr<ZSTD_CCtx, deleter> _ctx;
    public:
        cctx() {
            _ctx.reset(ZSTD_createCCtx());
            if (!_ctx) {
                throw std::bad_alloc();
            }
        }
        ZSTD_CCtx* get() {
            return _ctx.get();
        }
    };

    static ZSTD_CCtx* get_cctx() {
        static thread_local cctx shared_cctx;
        return shared_cctx.get();
    }
    static ZSTD_DCtx* get_dctx() {
        static thread_local dctx shared_dctx;
        return shared_dctx.get();
    }
    compressor::dict_ptr _dict;

public:
    zstd_processor_with_dict(compressor::dict_ptr d)
        : compressor(compressor::zstd_class_name)
        , _dict(std::move(d)) {    
    }

    size_t uncompress(const char* input, size_t input_len, char* output, size_t output_len) const override {
        auto dctx = get_dctx();
        try {
            size_t ret = ZSTD_decompress_usingDDict(dctx, output, output_len, input, input_len, _dict->get()->dict().zstd_ddict.get());
            check_zstd(ret, "ZSTD_decompress_usingDDict");
            return ret;
        } catch(...) {
            ZSTD_DCtx_reset(get_dctx(), ZSTD_reset_session_only);
            throw;
        }
    }
    size_t compress(const char* input, size_t input_len, char* output, size_t output_len) const override {
        auto cctx = get_cctx();
        try {
            size_t ret = ZSTD_compress_usingCDict(cctx, output, output_len, input, input_len, _dict->get()->dict().zstd_cdict.get());
            check_zstd(ret, "ZSTD_decompress_usingCDict");
            return ret;
        } catch (...) {
            ZSTD_CCtx_reset(cctx, ZSTD_reset_session_only);
            throw;
        }
    }
    size_t compress_max_size(size_t input_len) const override {
        return ZSTD_compressBound(input_len);
    }

    std::set<sstring> option_names() const override {
        return {};
    }
    std::map<sstring, sstring> options() const override {
        return {};
    }
};

compressor::ptr_type make_zstd_compressor_with_dict(compressor::dict_ptr d) {
    return seastar::make_shared<zstd_processor_with_dict>(d);
}