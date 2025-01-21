/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <map>
#include <optional>
#include <set>

#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include "seastarx.hh"

class compressor {
    sstring _name;
public:
    compressor(sstring);

    virtual ~compressor() {}

    /**
     * Unpacks data in "input" to output. If output_len is of insufficient size,
     * exception is thrown. I.e. you should keep track of the uncompressed size.
     */
    virtual size_t uncompress(const char* input, size_t input_len, char* output,
                    size_t output_len) const = 0;
    /**
     * Packs data in "input" to output. If output_len is of insufficient size,
     * exception is thrown. Maximum required size is obtained via "compress_max_size"
     */
    virtual size_t compress(const char* input, size_t input_len, char* output,
                    size_t output_len) const = 0;
    /**
     * Returns the maximum output size for compressing data on "input_len" size.
     */
    virtual size_t compress_max_size(size_t input_len) const = 0;

    /**
     * Returns accepted option names for this compressor
     */
    virtual std::set<sstring> option_names() const;
    /**
     * Returns original options used in instantiating this compressor
     */
    virtual std::map<sstring, sstring> options() const;

    /**
     * Compressor class name.
     */
    const sstring& name() const {
        return _name;
    }

    // to cheaply bridge sstable compression options / maps
    using opt_string = std::optional<sstring>;
    using opt_getter = std::function<opt_string(const sstring&)>;
    using ptr_type = shared_ptr<compressor>;

    static ptr_type create(const sstring& name, const opt_getter&);
    static ptr_type create(const std::map<sstring, sstring>&);

    static thread_local const ptr_type lz4;
    static thread_local const ptr_type snappy;
    static thread_local const ptr_type deflate;

    static const sstring namespace_prefix;
};

template<typename BaseType, typename... Args>
class class_registry;

using compressor_ptr = compressor::ptr_type;
using compressor_registry = class_registry<compressor, const typename compressor::opt_getter&>;

class compression_parameters {
public:
    enum class algorithm {
        lz4,
        zstd,
        snappy,
        deflate,
        none,
    };
    static constexpr std::string_view algorithm_names[] = {
        "org.apache.cassandra.io.compress.LZ4Compressor",
        "org.apache.cassandra.io.compress.ZstdCompressor",
        "org.apache.cassandra.io.compress.SnappyCompressor",
        "org.apache.cassandra.io.compress.DeflateCompressor",
    };
    // The duplication here is unfortunate, but C++ can't really do better than this.
    static_assert(std::size(algorithm_names) == int(algorithm::none));
    static_assert(algorithm_names[int(algorithm::lz4)] == "org.apache.cassandra.io.compress.LZ4Compressor");
    static_assert(algorithm_names[int(algorithm::zstd)] == "org.apache.cassandra.io.compress.ZstdCompressor");
    static_assert(algorithm_names[int(algorithm::snappy)] == "org.apache.cassandra.io.compress.SnappyCompressor");
    static_assert(algorithm_names[int(algorithm::deflate)] == "org.apache.cassandra.io.compress.DeflateCompressor");

    static constexpr int32_t DEFAULT_CHUNK_LENGTH = 4 * 1024;
    static constexpr double DEFAULT_CRC_CHECK_CHANCE = 1.0;

    static const sstring SSTABLE_COMPRESSION;
    static const sstring CHUNK_LENGTH_KB;
    static const sstring CHUNK_LENGTH_KB_ERR;
    static const sstring CRC_CHECK_CHANCE;
private:
    std::map<sstring, sstring> _raw_options;
    algorithm _algorithm;
    std::optional<int> _chunk_length;
    std::optional<double> _crc_check_chance;
    std::optional<int> _zstd_compression_level;
public:
    compression_parameters();
    compression_parameters(algorithm);
    compression_parameters(const std::map<sstring, sstring>& options);
    ~compression_parameters();

    int32_t chunk_length() const { return _chunk_length.value_or(int(DEFAULT_CHUNK_LENGTH)); }
    double crc_check_chance() const { return _crc_check_chance.value_or(double(DEFAULT_CRC_CHECK_CHANCE)); }
    algorithm get_algorithm() const { return _algorithm; }

    void validate();
    const std::map<sstring, sstring>& get_options() const {
        return _raw_options;
    }

    compressor_ptr get_compressor() const {
        return compressor::create(get_options());
    }
    static compression_parameters no_compression() {
        return compression_parameters(algorithm::none);
    }
    bool operator==(const compression_parameters&) const;
private:
    static void validate_options(const std::map<sstring, sstring>&);
    static algorithm name_to_algorithm(std::string_view name); 
};
