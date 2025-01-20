#/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: ScyllaDB-Proprietary
 */

#pragma once

#include "seastar/core/sharded.hh"
#include "seastar/core/shared_ptr.hh"
#include "utils/shared_dict.hh"
#include "utils/bit_cast.hh"

class shared_dict_registry : public peering_sharded_service<shared_dict_registry> {
    struct sha_to_map_key {
        std::size_t operator()(const utils::sha256_type& k) const {
            return read_unaligned<uint64_t>(k.data());
        }
    };
public:
    class entry : public enable_lw_shared_from_this<entry> {
        shared_dict_registry& _owner;
        utils::sha256_type _key;
        utils::shared_dict _dict;
    public:
        entry(shared_dict_registry& owner, utils::sha256_type key, utils::shared_dict dict)
            : _owner(owner)
            , _key(key)
            , _dict(std::move(dict))
        {}
        ~entry() {
            _owner._entries.erase(_key);
        }
        const utils::shared_dict& dict() {
            return _dict;
        }
    };
    std::unordered_map<utils::sha256_type, entry*, sha_to_map_key> _entries;
public:
    future<foreign_ptr<lw_shared_ptr<entry>>> get_dict(std::span<const std::byte> dict) {
        auto sha = utils::get_sha256(dict);
        int owning_shard = read_unaligned<uint64_t>(sha.data()) % smp::count;
        return container().invoke_on(owning_shard, [dict, sha] (shared_dict_registry& local) {
            if (auto it = local._entries.find(sha); it != local._entries.end()) {
                return make_foreign(it->second->shared_from_this());
            } else {
                constexpr int ZSTD_DEFAULT_COMPRESSION_LEVEL = 3;
                auto new_dict = utils::shared_dict(dict, {}, {}, ZSTD_DEFAULT_COMPRESSION_LEVEL, sha);
                auto new_entry = make_lw_shared<entry>(local, sha, std::move(new_dict));
                local._entries.emplace(sha, new_entry.get());
                return make_foreign(new_entry);
            }
        });
    }
};