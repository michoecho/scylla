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

#include "compress.hh"
#include "exceptions/exceptions.hh"
#include "utils/class_registrator.hh"

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
};

class snappy_processor: public compressor {
public:
    using compressor::compressor;

    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress_max_size(size_t input_len) const override;
};

class deflate_processor: public compressor {
public:
    using compressor::compressor;

    size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const override;
    size_t compress_max_size(size_t input_len) const override;
};

compressor::compressor(sstring name)
    : _name(std::move(name))
{}

std::set<sstring> compressor::option_names() const {
    return {};
}

std::map<sstring, sstring> compressor::options() const {
    return {};
}

compressor::ptr_type compressor::create(const sstring& name, const opt_getter& opts) {
    if (name.empty()) {
        return {};
    }

    qualified_name qn(make_name(""), name);

    for (auto& c : { lz4, snappy, deflate }) {
        if (c->name() == static_cast<const sstring&>(qn)) {
            return c;
        }
    }

    return compressor_registry::create(qn, opts);
}

shared_ptr<compressor> compressor::create(const std::map<sstring, sstring>& options) {
    auto i = options.find(compression_parameters::SSTABLE_COMPRESSION);
    if (i != options.end() && !i->second.empty()) {
        return create(i->second, [&options](const sstring& key) -> opt_string {
            auto i = options.find(key);
            if (i == options.end()) {
                return std::nullopt;
            }
            return { i->second };
        });
    }
    return {};
}

thread_local const shared_ptr<compressor> compressor::lz4 = ::make_shared<lz4_processor>(make_name("LZ4Compressor"));
thread_local const shared_ptr<compressor> compressor::snappy = ::make_shared<snappy_processor>(make_name("SnappyCompressor"));
thread_local const shared_ptr<compressor> compressor::deflate = ::make_shared<deflate_processor>(make_name("DeflateCompressor"));

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

