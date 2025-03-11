/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "replica/database.hh"
#include "service/raft/raft_group0_client.hh"

class sstable_dict_autotrainer {
    service::storage_service& _ss;
    service::raft_group0_client& _group0_client;
    future<> tick();
public:
    sstable_dict_autotrainer(service::storage_service&, service::raft_group0_client&);
    future<> run();
};

future<float> try_one_compression_config(
    sstable_compressor_factory& factory,
    schema_ptr initial_schema,
    const compression_parameters& params,
    const utils::chunked_vector<bytes>& validation_samples
);

future<float> try_one_compression_config(
    std::span<std::byte> dict,
    schema_ptr initial_schema,
    const compression_parameters& params,
    const utils::chunked_vector<bytes>& validation_samples
);
