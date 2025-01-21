#include <seastar/core/coroutine.hh>
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
#include "schema/schema_fwd.hh"


class abstract_shared_dict_registry;

class entry : public enable_lw_shared_from_this<entry> {
    abstract_shared_dict_registry& _owner;
    utils::sha256_type _key;
    utils::shared_dict _dict;
public:
    entry(abstract_shared_dict_registry& owner, utils::sha256_type key, utils::shared_dict dict)
        : _owner(owner)
        , _key(key)
        , _dict(std::move(dict))
    {}
    ~entry();
    const utils::shared_dict& dict() {
        return _dict;
    }
};

class abstract_shared_dict_registry {
public:
    using foreign_entry_ptr = foreign_ptr<lw_shared_ptr<entry>>;
    virtual future<> set_recommended(table_id t, std::span<const std::byte> dict) = 0;
    virtual future<foreign_entry_ptr> get_dict_by_content(std::span<const std::byte> dict) = 0;
    virtual lw_shared_ptr<foreign_entry_ptr> get_recommended_dict_by_table(table_id t) = 0;
    virtual future<> set_default_dict(std::span<const std::byte> dict) = 0;
    virtual void erase_by_sha(utils::sha256_type) = 0;
};

class mock_shared_dict_registry : public abstract_shared_dict_registry {
    struct sha_to_map_key {
        std::size_t operator()(const utils::sha256_type& k) const {
            return read_unaligned<uint64_t>(k.data());
        }
    };
public:
    std::unordered_map<utils::sha256_type, entry*, sha_to_map_key> _owned_entries_by_sha;
public:
    future<> set_default_dict(std::span<const std::byte> dict) override {
        return make_ready_future<>();
    }
    future<> set_recommended(table_id t, std::span<const std::byte> dict) override {
        return make_ready_future<>();
    }
    future<foreign_entry_ptr> get_dict_by_content(std::span<const std::byte> dict) override {
        auto sha = utils::get_sha256(dict);
        if (auto it = _owned_entries_by_sha.find(sha); it != _owned_entries_by_sha.end()) {
            return make_ready_future<foreign_entry_ptr>(make_foreign(it->second->shared_from_this()));
        } else {
            constexpr int ZSTD_DEFAULT_COMPRESSION_LEVEL = 3;
            auto new_dict = utils::shared_dict(dict, {}, {}, ZSTD_DEFAULT_COMPRESSION_LEVEL, sha);
            auto new_entry = make_lw_shared<entry>(*this, sha, std::move(new_dict));
            _owned_entries_by_sha.emplace(sha, new_entry.get());
            return make_ready_future<foreign_entry_ptr>(make_foreign(new_entry));
        }
    }
    lw_shared_ptr<foreign_entry_ptr> get_recommended_dict_by_table(table_id t) override {
        return nullptr;
    }
    void erase_by_sha(utils::sha256_type sha) override {
        _owned_entries_by_sha.erase(sha);
    }
};

class shared_dict_registry : public peering_sharded_service<shared_dict_registry>, public abstract_shared_dict_registry {
    struct sha_to_map_key {
        std::size_t operator()(const utils::sha256_type& k) const {
            return read_unaligned<uint64_t>(k.data());
        }
    };
public:
    using abstract_shared_dict_registry::foreign_entry_ptr;
    std::unordered_map<utils::sha256_type, entry*, sha_to_map_key> _owned_entries_by_sha;
    std::unordered_map<table_id, lw_shared_ptr<foreign_entry_ptr>> _entries_by_table_id;
    lw_shared_ptr<foreign_entry_ptr> _default_dict;
    semaphore _semaphore{1};
public:
    future<> set_default_dict(std::span<const std::byte> dict) override {
        co_await container().invoke_on(0, coroutine::lambda([dict] (shared_dict_registry& coordinator) -> future<> {
            auto ticket = co_await get_units(coordinator._semaphore, 1);
            co_await coordinator.container().invoke_on_all(coroutine::lambda([dict] (shared_dict_registry& local) -> future<> {
                auto fptr = co_await local.get_dict_by_content(dict);
                local._default_dict = make_lw_shared(std::move(fptr));
            }));
        }));
    }
    future<> set_recommended(table_id t, std::span<const std::byte> dict) override {
        co_await container().invoke_on(0, coroutine::lambda([dict, t] (shared_dict_registry& coordinator) -> future<> {
            auto ticket = co_await get_units(coordinator._semaphore, 1);
            co_await coordinator.container().invoke_on_all(coroutine::lambda([dict, t] (shared_dict_registry& local) -> future<> {
                auto fptr = co_await local.get_dict_by_content(dict);
                local._entries_by_table_id.emplace(t, make_lw_shared(std::move(fptr)));
            }));
        }));
    }
    future<foreign_entry_ptr> get_dict_by_content(std::span<const std::byte> dict) override {
        auto sha = utils::get_sha256(dict);
        int owning_shard = read_unaligned<uint64_t>(sha.data()) % smp::count;
        return container().invoke_on(owning_shard, [dict, sha] (shared_dict_registry& local) {
            if (auto it = local._owned_entries_by_sha.find(sha); it != local._owned_entries_by_sha.end()) {
                return make_foreign(it->second->shared_from_this());
            } else {
                constexpr int ZSTD_DEFAULT_COMPRESSION_LEVEL = 3;
                auto new_dict = utils::shared_dict(dict, {}, {}, ZSTD_DEFAULT_COMPRESSION_LEVEL, sha);
                auto new_entry = make_lw_shared<entry>(local, sha, std::move(new_dict));
                local._owned_entries_by_sha.emplace(sha, new_entry.get());
                return make_foreign(new_entry);
            }
        });
    }
    lw_shared_ptr<foreign_entry_ptr> get_recommended_dict_by_table(table_id t) override {
        if (auto it = _entries_by_table_id.find(t); it != _entries_by_table_id.end()) {
            return it->second;
        } else {
            return _default_dict;
        }
    }
    void erase_by_sha(utils::sha256_type sha) override {
        _owned_entries_by_sha.erase(sha);
    }
};

inline entry::~entry() {
    _owner.erase_by_sha(_key);
}