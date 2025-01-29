/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

// We need to use experimental features of the zstd library (to allocate compression/decompression context),
// which are available only when the library is linked statically.
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>
#include <lz4.h>
#include <zlib.h>
#include <snappy-c.h>
#include <seastar/util/log.hh>
#include <seastar/core/sharded.hh>

#include "compress.hh"
#include "exceptions/exceptions.hh"
#include "utils/class_registrator.hh"
#include "sstables/compressor_registry.hh"

#include "utils/reusable_buffer.hh"
#include <concepts>

class compressor_registry;

// SHA256
using dict_id = std::array<std::byte, 32>;
class compressor_registry_impl;

namespace utils {
    dict_id get_sha256(std::span<const std::byte> in);
};

class raw_dict : public enable_lw_shared_from_this<raw_dict> {
    compressor_registry_impl& _owner;
    dict_id _id;
    std::vector<std::byte> _dict;
public:
    raw_dict(compressor_registry_impl& owner, dict_id key, std::span<const std::byte> dict)
        : _owner(owner)
        , _id(key)
        , _dict(dict.begin(), dict.end())
    {}
    ~raw_dict();
    const std::span<const std::byte> raw() const {
        return _dict;
    }
    dict_id id() const {
        return _id;
    }
};

class zstd_ddict : public enable_lw_shared_from_this<zstd_ddict> {
    compressor_registry_impl& _owner;
    lw_shared_ptr<const raw_dict> _raw;
    std::unique_ptr<ZSTD_DDict, decltype(&ZSTD_freeDDict)> _dict;
public:
    zstd_ddict(compressor_registry_impl& owner, lw_shared_ptr<const raw_dict> raw)
        : _owner(owner)
        , _raw(raw)
        , _dict(ZSTD_createDDict_byReference(_raw->raw().data(), _raw->raw().size()), ZSTD_freeDDict)
    {
        if (!_dict) {
            throw std::bad_alloc();
        }
    }
    ~zstd_ddict();
    auto dict() const {
        return _dict.get();
    }
};

class zstd_cdict : public enable_lw_shared_from_this<zstd_cdict> {
    compressor_registry_impl& _owner;
    lw_shared_ptr<const raw_dict> _raw;
    int _level;
    std::unique_ptr<ZSTD_CDict, decltype(&ZSTD_freeCDict)> _dict;
public:
    zstd_cdict(compressor_registry_impl& owner, lw_shared_ptr<const raw_dict> raw, int level)
        : _owner(owner)
        , _raw(raw)
        , _level(level)
        , _dict(ZSTD_createCDict_byReference(_raw->raw().data(), _raw->raw().size(), level), ZSTD_freeCDict)
    {
        if (!_dict) {
            throw std::bad_alloc();
        }
    }
    ~zstd_cdict();
    auto dict() const {
        return _dict.get();
    }
};

class raw_dict;
class zstd_ddict;
class zstd_cdict;

template <typename T>
std::vector<foreign_ptr<T>> make_foreign_ptrs(T a_shared_ptr) {
    std::vector<foreign_ptr<T>> result(smp::count);
    std::ranges::generate(result, [&] () { return make_foreign(a_shared_ptr); });
    return result;
}

class compressor_registry_impl : public compressor_registry {
    struct zstd_cdict_id {
        dict_id id;
        int level;
        std::strong_ordering operator<=>(const zstd_cdict_id&) const = default;
    };
    std::map<dict_id, const raw_dict*> _raw_dicts;
    std::map<zstd_cdict_id, const zstd_cdict*> _zstd_cdicts;
    std::map<dict_id, const zstd_ddict*> _zstd_ddicts;
    std::map<table_id, lw_shared_ptr<const raw_dict>> _recommended;

    lw_shared_ptr<const raw_dict> get_canonical_ptr(std::span<const std::byte> dict) {
        SCYLLA_ASSERT(this_shard_id() == 0);
        auto id = utils::get_sha256(dict);
        if (auto it = _raw_dicts.find(id); it != _raw_dicts.end()) {
            return it->second->shared_from_this();
        } else {
            auto p = make_lw_shared<const raw_dict>(*this, id, dict);
            _raw_dicts.emplace(id, p.get());
            return p;
        }
    }
    using zstd_dicts = std::pair<
        foreign_ptr<lw_shared_ptr<const zstd_ddict>>,
        foreign_ptr<lw_shared_ptr<const zstd_cdict>>
    >;
    zstd_dicts get_zstd_dicts(lw_shared_ptr<const raw_dict> raw, int level) {
        SCYLLA_ASSERT(this_shard_id() == 0);
        lw_shared_ptr<const zstd_ddict> ddict;
        lw_shared_ptr<const zstd_cdict> cdict;
        if (auto it = _zstd_ddicts.find(raw->id()); it != _zstd_ddicts.end()) {
            ddict = it->second->shared_from_this();
        } else {
            ddict = make_lw_shared<zstd_ddict>(*this, raw);
            _zstd_ddicts.emplace(raw->id(), ddict.get());
        }
        if (auto it = _zstd_cdicts.find({raw->id(), level}); it != _zstd_cdicts.end()) {
            cdict = it->second->shared_from_this();
        } else {
            cdict = make_lw_shared<zstd_cdict>(*this, raw, level);
            _zstd_cdicts.emplace(zstd_cdict_id{raw->id(), level}, cdict.get());
        }
        return {make_foreign(std::move(ddict)), make_foreign(std::move(cdict))};
    }
    future<zstd_dicts> get_zstd_dicts(std::span<const std::byte> dict, int level) {
        return smp::submit_to(0, [this, dict, level] -> zstd_dicts {
            auto raw = get_canonical_ptr(dict);
            return get_zstd_dicts(raw, level);
        });
    }
    future<zstd_dicts> get_zstd_dicts(table_id t, int level) {
        return smp::submit_to(0, [this, t, level] -> zstd_dicts {
            auto rec_it = _recommended.find(t);
            if (rec_it == _recommended.end()) {
                return {};
            }
            return get_zstd_dicts(rec_it->second, level);
        });
    }
    
public:
    compressor_registry_impl() {
        SCYLLA_ASSERT(this_shard_id() == 0);
    }
    compressor_registry_impl(compressor_registry_impl&&) = delete;
    ~compressor_registry_impl() = default;
    void invalidate_raw_dict(dict_id id) {
        SCYLLA_ASSERT(this_shard_id() == 0);
        _raw_dicts.erase(id);
    }
    void invalidate_zstd_cdict(dict_id id, int level) {
        SCYLLA_ASSERT(this_shard_id() == 0);
        _zstd_cdicts.erase({id, level});
    }
    void invalidate_zstd_ddict(dict_id id) {
        SCYLLA_ASSERT(this_shard_id() == 0);
        _zstd_ddicts.erase(id);
    }
    virtual future<> set_recommended_dict(table_id t, std::span<const std::byte> dict) {
        return smp::submit_to(0, [this, t, dict] {
            auto canonical_ptr = get_canonical_ptr(dict);
            _recommended.emplace(t, canonical_ptr);
        });
    }
    virtual future<std::unique_ptr<compressor>> make_compressor_for_schema(schema_ptr s);
    virtual future<> make_compressor_for_reading(sstables::compression&);
};

raw_dict::~raw_dict() {
    _owner.invalidate_raw_dict(_id);
}

zstd_cdict::~zstd_cdict() {
    _owner.invalidate_zstd_cdict(_raw->id(), _level);
}

zstd_ddict::~zstd_ddict() {
    _owner.invalidate_zstd_ddict(_raw->id());
}

sstring compressor::make_name(std::string_view short_name) {
    return seastar::format("org.apache.cassandra.io.compress.{}", short_name);
}

class lz4_processor: public compressor {
public:
    using compressor::compressor;

    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress_max_size(size_t input_len) const override;
    const char* name() const override;
};

class snappy_processor: public compressor {
public:
    using compressor::compressor;

    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress_max_size(size_t input_len) const override;
    const char* name() const override;
};

class deflate_processor: public compressor {
public:
    using compressor::compressor;

    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress_max_size(size_t input_len) const override;
    const char* name() const override;
};

static const sstring COMPRESSION_LEVEL = "compression_level";
static const size_t ZSTD_DCTX_SIZE = ZSTD_estimateDCtxSize();
class zstd_processor : public compressor {
    int _compression_level = 3;
    size_t _cctx_size;

    static auto with_dctx(std::invocable<ZSTD_DCtx*> auto f) {
        // The decompression context has a fixed size of ~128 KiB,
        // so we don't bother ever resizing it the way we do with
        // the compression context.
        static thread_local std::unique_ptr<char[]> buf = std::invoke([&] {
            auto ptr = std::unique_ptr<char[]>(new char[ZSTD_DCTX_SIZE]);
            auto dctx = ZSTD_initStaticDCtx(ptr.get(), ZSTD_DCTX_SIZE);
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
    zstd_processor(const std::map<sstring, sstring>& options);

    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress_max_size(size_t input_len) const override;

    std::set<sstring> option_names() const override;
    std::map<sstring, sstring> options() const override;
    const char* name() const override {
        static const sstring COMPRESSOR_NAME = make_name("ZstdCompressor");
        return COMPRESSOR_NAME.c_str();
    }
};

zstd_processor::zstd_processor(const opt_getter& opts) {
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

zstd_processor::zstd_processor(const std::map<sstring, sstring>& options)
    : zstd_processor([&] (const sstring& opt) -> std::optional<sstring> {
        if (auto it = options.find(opt); it != options.end()) {
            return it->second;
        } else {
            return std::nullopt;
        }
    })
{}

std::unique_ptr<compressor> make_unique_zstd_compressor(const std::map<sstring, sstring>& options) {
    return std::make_unique<zstd_processor>(options);
}

compressor_ptr make_zstd_compressor(const std::map<sstring, sstring>& options) {
    return seastar::make_shared<zstd_processor>(options);
}

future<std::unique_ptr<compressor>> compressor_registry_impl::make_compressor_for_schema(schema_ptr s) {
    // FIXME: use recommended dictionaries.
    auto params = s->get_compressor_params();
    using algorithm = compression_parameters::algorithm;
    switch (params.get_algorithm()) {
    case algorithm::lz4:
        co_return std::make_unique<lz4_processor>();
    case algorithm::deflate:
        co_return std::make_unique<deflate_processor>();
    case algorithm::snappy:
        co_return std::make_unique<snappy_processor>();
    case algorithm::zstd:
        co_return make_unique_zstd_compressor(s->get_compressor_params().get_options());
    case algorithm::none:
        co_return nullptr;
    }
    abort();
}

future<> compressor_registry_impl::make_compressor_for_reading(sstables::compression& c) {
    abort();
}

std::set<sstring> compressor::option_names() const {
    return {};
}

std::map<sstring, sstring> compressor::options() const {
    return {};
}

compressor::ptr_type compressor::create(const compression_parameters& cp) {
    using algorithm = compression_parameters::algorithm;
    switch (cp.get_algorithm()) {
    case algorithm::lz4:
        return lz4;
    case algorithm::deflate:
        return deflate;
    case algorithm::snappy:
        return snappy;
    case algorithm::zstd:
        return make_zstd_compressor(cp.get_options());
    case algorithm::none:
        return nullptr;
    }
    abort();
}

thread_local const shared_ptr<compressor> compressor::lz4 = ::make_shared<lz4_processor>();
thread_local const shared_ptr<compressor> compressor::snappy = ::make_shared<snappy_processor>();
thread_local const shared_ptr<compressor> compressor::deflate = ::make_shared<deflate_processor>();

const sstring compression_parameters::SSTABLE_COMPRESSION = "sstable_compression";
const sstring compression_parameters::CHUNK_LENGTH_KB = "chunk_length_in_kb";
const sstring compression_parameters::CHUNK_LENGTH_KB_ERR = "chunk_length_kb";
const sstring compression_parameters::CRC_CHECK_CHANCE = "crc_check_chance";

compression_parameters::compression_parameters()
    : compression_parameters(algorithm::lz4)
{}

compression_parameters::~compression_parameters()
{}

compression_parameters::compression_parameters(algorithm alg)
    : compression_parameters::compression_parameters(
        alg == algorithm::none
        ? std::map<sstring, sstring>{}
        : std::map<sstring, sstring>{{sstring(SSTABLE_COMPRESSION), sstring(algorithm_names[int(alg)])},
    })
{}

auto compression_parameters::name_to_algorithm(std::string_view name) -> algorithm {
    if (name.empty()) {
        return algorithm::none;
    }
    auto qn = sstring(qualified_name(compressor::make_name(""), name));
    auto it = std::ranges::find(algorithm_names, qn);
    if (it == std::end(algorithm_names)) {
        throw std::runtime_error(std::format("Unknown sstable compression algorithm: {}", name));
    }
    return algorithm(it - std::begin(algorithm_names));
}

compression_parameters::compression_parameters(const std::map<sstring, sstring>& options) {
    _raw_options = options;
    std::set<sstring> used_options;
    auto get_option = [&options, &used_options] (const sstring& x) -> const sstring* {
        used_options.insert(x);
        if (auto it = options.find(x); it != options.end()) {
            return &it->second;
        }
        return nullptr;
    };

    if (auto v = get_option(SSTABLE_COMPRESSION)) {
        _algorithm = name_to_algorithm(*v);
    } else {
        _algorithm = algorithm::none;
    }

    const sstring* chunk_length = nullptr;
    if (auto v = get_option(CHUNK_LENGTH_KB_ERR)) {
        chunk_length = v;
    }
    if (auto v = get_option(CHUNK_LENGTH_KB)) {
        chunk_length = v;
    }
    if (chunk_length) {
        try {
            _chunk_length = std::stoi(*chunk_length) * 1024;
        } catch (const std::exception& e) {
            throw exceptions::syntax_exception(sstring("Invalid integer value ") + *chunk_length + " for " + CHUNK_LENGTH_KB);
        }
    }

    if (auto v = get_option(CRC_CHECK_CHANCE)) {
        try {
            _crc_check_chance = std::stod(*v);
        } catch (const std::exception& e) {
            throw exceptions::syntax_exception(sstring("Invalid double value ") + *v + "for " + CRC_CHECK_CHANCE);
        }
    }

    switch (_algorithm) {
    case algorithm::zstd:
        if (auto v = get_option("compression_level")) {
            try {
                _zstd_compression_level = std::stoi(*v);
            } catch (const std::exception&) {
                throw exceptions::configuration_exception(format("Invalid compression_level '{}', not a valid integer: (reason: {}).", *v, std::current_exception()));
            }
        }
        break;
    default:
    }

    for (const auto& o : options) {
        if (!used_options.contains(o.first)) {
            throw exceptions::configuration_exception(format("Unknown compression option '{}'.", o.first));
        }
    }
}

bool compression_parameters::operator==(const compression_parameters& other) const {
    return _raw_options == other._raw_options;
}

void compression_parameters::validate() {
    if (_chunk_length) {
        auto chunk_length = _chunk_length.value();
        if (chunk_length <= 0) {
            throw exceptions::configuration_exception(
                fmt::format("Invalid negative or null for {}/{}", CHUNK_LENGTH_KB, CHUNK_LENGTH_KB_ERR));
        }
        // _chunk_length must be a power of two
        if (chunk_length & (chunk_length - 1)) {
            throw exceptions::configuration_exception(
                fmt::format("{}/{} must be a power of 2.", CHUNK_LENGTH_KB, CHUNK_LENGTH_KB_ERR));
        }
        // Excessive _chunk_length is pointless and can lead to allocation
        // failures (see issue #9933)
        if (chunk_length > 128 * 1024) {
            throw exceptions::configuration_exception(
                fmt::format("{}/{} must be 128 or less.", CHUNK_LENGTH_KB, CHUNK_LENGTH_KB_ERR));
        }
    }
    if (_crc_check_chance && (_crc_check_chance.value() < 0.0 || _crc_check_chance.value() > 1.0)) {
        throw exceptions::configuration_exception(sstring(CRC_CHECK_CHANCE) + " must be between 0.0 and 1.0.");
    }
    if (_zstd_compression_level) {
        if (*_zstd_compression_level != std::clamp<int>(*_zstd_compression_level, ZSTD_minCLevel(), ZSTD_maxCLevel())) {
            throw exceptions::configuration_exception(fmt::format("compression_level is {}, but it must be between {} and {}.", *_zstd_compression_level, ZSTD_minCLevel(), ZSTD_maxCLevel()));
        }
    }
}

size_t lz4_processor::uncompress(const char* input, size_t input_len,
                char* output, size_t output_len) const {
    // We use LZ4_decompress_safe(). According to the documentation, the
    // function LZ4_decompress_fast() is slightly faster, but maliciously
    // crafted compressed data can cause it to overflow the output buffer.
    // Theoretically, our compressed data is created by us so is not malicious
    // (and accidental corruption is avoided by the compressed-data checksum),
    // but let's not take that chance for now, until we've actually measured
    // the performance benefit that LZ4_decompress_fast() would bring.

    // Cassandra's LZ4Compressor prepends to the chunk its uncompressed length
    // in 4 bytes little-endian (!) order. We don't need this information -
    // we already know the uncompressed data is at most the given chunk size
    // (and usually is exactly that, except in the last chunk). The advance
    // knowledge of the uncompressed size could be useful if we used
    // LZ4_decompress_fast(), but we prefer LZ4_decompress_safe() anyway...
    input += 4;
    input_len -= 4;

    auto ret = LZ4_decompress_safe(input, output, input_len, output_len);
    if (ret < 0) {
        throw std::runtime_error("LZ4 uncompression failure");
    }
    return ret;
}

size_t lz4_processor::compress(const char* input, size_t input_len,
                char* output, size_t output_len) const {
    if (output_len < LZ4_COMPRESSBOUND(input_len) + 4) {
        throw std::runtime_error("LZ4 compression failure: length of output is too small");
    }
    // Write input_len (32-bit data) to beginning of output in little-endian representation.
    output[0] = input_len & 0xFF;
    output[1] = (input_len >> 8) & 0xFF;
    output[2] = (input_len >> 16) & 0xFF;
    output[3] = (input_len >> 24) & 0xFF;
    auto ret = LZ4_compress_default(input, output + 4, input_len, LZ4_compressBound(input_len));
    if (ret == 0) {
        throw std::runtime_error("LZ4 compression failure: LZ4_compress() failed");
    }
    return ret + 4;
}

size_t lz4_processor::compress_max_size(size_t input_len) const {
    return LZ4_COMPRESSBOUND(input_len) + 4;
}

const char* lz4_processor::name() const {
    const static std::string name = make_name("LZ4Compressor");
    return name.c_str();
}

size_t deflate_processor::uncompress(const char* input,
                size_t input_len, char* output, size_t output_len) const {
    z_stream zs;
    zs.zalloc = Z_NULL;
    zs.zfree = Z_NULL;
    zs.opaque = Z_NULL;
    zs.avail_in = 0;
    zs.next_in = Z_NULL;
    if (inflateInit(&zs) != Z_OK) {
        throw std::runtime_error("deflate uncompression init failure");
    }
    // yuck, zlib is not const-correct, and also uses unsigned char while we use char :-(
    zs.next_in = reinterpret_cast<unsigned char*>(const_cast<char*>(input));
    zs.avail_in = input_len;
    zs.next_out = reinterpret_cast<unsigned char*>(output);
    zs.avail_out = output_len;
    auto res = inflate(&zs, Z_FINISH);
    inflateEnd(&zs);
    if (res == Z_STREAM_END) {
        return output_len - zs.avail_out;
    } else {
        throw std::runtime_error("deflate uncompression failure");
    }
}

size_t deflate_processor::compress(const char* input,
                size_t input_len, char* output, size_t output_len) const {
    z_stream zs;
    zs.zalloc = Z_NULL;
    zs.zfree = Z_NULL;
    zs.opaque = Z_NULL;
    zs.avail_in = 0;
    zs.next_in = Z_NULL;
    if (deflateInit(&zs, Z_DEFAULT_COMPRESSION) != Z_OK) {
        throw std::runtime_error("deflate compression init failure");
    }
    zs.next_in = reinterpret_cast<unsigned char*>(const_cast<char*>(input));
    zs.avail_in = input_len;
    zs.next_out = reinterpret_cast<unsigned char*>(output);
    zs.avail_out = output_len;
    auto res = ::deflate(&zs, Z_FINISH);
    deflateEnd(&zs);
    if (res == Z_STREAM_END) {
        return output_len - zs.avail_out;
    } else {
        throw std::runtime_error("deflate compression failure");
    }
}

size_t deflate_processor::compress_max_size(size_t input_len) const {
    z_stream zs;
    zs.zalloc = Z_NULL;
    zs.zfree = Z_NULL;
    zs.opaque = Z_NULL;
    zs.avail_in = 0;
    zs.next_in = Z_NULL;
    if (deflateInit(&zs, Z_DEFAULT_COMPRESSION) != Z_OK) {
        throw std::runtime_error("deflate compression init failure");
    }
    auto res = deflateBound(&zs, input_len);
    deflateEnd(&zs);
    return res;
}

const char* deflate_processor::name() const {
    const static std::string name = make_name("DeflateCompressor");
    return name.c_str();
}

size_t snappy_processor::uncompress(const char* input, size_t input_len,
                char* output, size_t output_len) const {
    if (snappy_uncompress(input, input_len, output, &output_len)
            == SNAPPY_OK) {
        return output_len;
    } else {
        throw std::runtime_error("snappy uncompression failure");
    }
}

size_t snappy_processor::compress(const char* input, size_t input_len,
                char* output, size_t output_len) const {
    auto ret = snappy_compress(input, input_len, output, &output_len);
    if (ret != SNAPPY_OK) {
        throw std::runtime_error("snappy compression failure: snappy_compress() failed");
    }
    return output_len;
}

size_t snappy_processor::compress_max_size(size_t input_len) const {
    return snappy_max_compressed_length(input_len);
}

const char* snappy_processor::name() const {
    const static std::string name = make_name("SnappyCompressor");
    return name.c_str();
}
