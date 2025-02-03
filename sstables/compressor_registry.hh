/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/future.hh>
#include "compress.hh"
#include "schema/schema_fwd.hh"

struct compressor_registry {  
    virtual ~compressor_registry() {}
    virtual future<> set_recommended_dict(table_id t, std::span<const std::byte> dict) = 0;
    virtual future<> set_default_dict(std::span<const std::byte> dict) = 0;
    virtual future<std::unique_ptr<compressor>> make_compressor_for_schema(schema_ptr) = 0;
    virtual future<> make_compressor_for_reading(sstables::compression&) = 0;
};

std::unique_ptr<compressor_registry> make_compressor_registry();
