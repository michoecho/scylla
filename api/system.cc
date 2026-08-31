/*
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "api/api_init.hh"
#include "api/api-doc/system.json.hh"
#include "api/api-doc/metrics.json.hh"
#include "db/config.hh"
#include "replica/database.hh"
#include "sstables/sstables_manager.hh"
#include "cql3/query_processor.hh"

#include <rapidjson/document.h>
#include <boost/lexical_cast.hpp>
#include <seastar/core/reactor.hh>
#include <seastar/core/scylla_tracer.hh>
#include <seastar/core/scylla_tracer_control.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/relabel_config.hh>
#include <seastar/http/exception.hh>
#include <seastar/util/short_streams.hh>
#include <seastar/util/short_streams.hh>

#include "utils/log.hh"

#include <filesystem>
#include <fstream>

extern logging::logger apilog;

namespace api {
using namespace seastar::httpd;

namespace hs = httpd::system_json;
namespace hm = httpd::metrics_json;

extern "C" void __attribute__((weak)) __llvm_profile_dump();
extern "C" const char * __attribute__((weak)) __llvm_profile_get_filename();
extern "C" void __attribute__((weak)) __llvm_profile_reset_counters();

void set_system(http_context& ctx, routes& r, sharded<cql3::query_processor>& qp) {
    hm::get_metrics_config.set(r, [](const_req req) {
        std::vector<hm::metrics_config> res;
        res.resize(seastar::metrics::get_relabel_configs().size());
        size_t i = 0;
        for (auto&& r : seastar::metrics::get_relabel_configs()) {
            res[i].action = r.action;
            res[i].target_label = r.target_label;
            res[i].replacement = r.replacement;
            res[i].separator = r.separator;
            res[i].source_labels = r.source_labels;
            res[i].regex = r.expr.str();
            i++;
        }
        return res;
    });

    hm::set_metrics_config.set(r, [](std::unique_ptr<http::request> req) -> future<json::json_return_type> {
        rapidjson::Document doc;
        auto content = co_await util::read_entire_stream_contiguous(*req->content_stream);
        doc.Parse(content.c_str());
        if (!doc.IsArray()) {
            throw bad_param_exception("Expected a json array");
        }
        std::vector<seastar::metrics::relabel_config> relabels;
        relabels.resize(doc.Size());
        for (rapidjson::SizeType i = 0; i < doc.Size(); i++) {
            const auto& element = doc[i];
            if (element.HasMember("source_labels")) {
                std::vector<std::string> source_labels;
                source_labels.resize(element["source_labels"].Size());

                for (size_t j = 0; j < element["source_labels"].Size(); j++) {
                    source_labels[j] = element["source_labels"][j].GetString();
                }
                relabels[i].source_labels = source_labels;
            }
            if (element.HasMember("action")) {
                relabels[i].action = seastar::metrics::relabel_config_action(element["action"].GetString());
            }
            if (element.HasMember("replacement")) {
                relabels[i].replacement = element["replacement"].GetString();
            }
            if (element.HasMember("separator")) {
                relabels[i].separator = element["separator"].GetString();
            }
            if (element.HasMember("target_label")) {
                relabels[i].target_label = element["target_label"].GetString();
            }
            if (element.HasMember("regex")) {
                relabels[i].expr = element["regex"].GetString();
            }
        }
        bool failed = false;
        co_await smp::invoke_on_all([&relabels, &failed] {
            return metrics::set_relabel_configs(relabels).then([&failed](const metrics::metric_relabeling_result& result) {
                if (result.metrics_relabeled_due_to_collision > 0) {
                    failed = true;
                }
                return;
            });
        });
        if (failed) {
            throw bad_param_exception("conflicts found during relabeling");
        }
        co_return seastar::json::json_void();
    });

    hs::get_system_uptime.set(r, [](const_req req) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(engine().uptime()).count();
    });

    hs::get_all_logger_names.set(r, [](const_req req) {
        return logging::logger_registry().get_all_logger_names();
    });

    hs::set_all_logger_level.set(r, [](const_req req) {
        try {
            logging::log_level level = boost::lexical_cast<logging::log_level>(std::string(req.get_query_param("level")));
            logging::logger_registry().set_all_loggers_level(level);
        } catch (boost::bad_lexical_cast& e) {
            throw bad_param_exception("Unknown logging level " + req.get_query_param("level"));
        }
        return json::json_void();
    });

    hs::get_logger_level.set(r, [](const_req req) {
        try {
            return logging::level_name(logging::logger_registry().get_logger_level(req.get_path_param("name")));
        } catch (std::out_of_range& e) {
            throw bad_param_exception("Unknown logger name " + req.get_path_param("name"));
        }
        // just to keep the compiler happy
        return sstring();
    });

    hs::set_logger_level.set(r, [](const_req req) {
        try {
            logging::log_level level = boost::lexical_cast<logging::log_level>(std::string(req.get_query_param("level")));
            logging::logger_registry().set_logger_level(req.get_path_param("name"), level);
        } catch (std::out_of_range& e) {
            throw bad_param_exception("Unknown logger name " + req.get_path_param("name"));
        } catch (boost::bad_lexical_cast& e) {
            throw bad_param_exception("Unknown logging level " + req.get_query_param("level"));
        }
        return json::json_void();
    });

    hs::write_log_message.set(r, [](const_req req) {
        try {
            logging::log_level level = boost::lexical_cast<logging::log_level>(std::string(req.get_query_param("level")));
            apilog.log(level, "/system/log: {}", std::string(req.get_query_param("message")));
        } catch (boost::bad_lexical_cast& e) {
            throw bad_param_exception("Unknown logging level " + req.get_query_param("level"));
        }
        return json::json_void();
    });

    // Switch every binary tracepoint in the process on or off.
    //
    // Not a setter but a rendezvous: flipping a tracepoint's static key
    // rewrites the branch instruction at its call site, and no shard may be
    // executing that instruction while it changes. seastar's
    // set_tracepoints_enabled() gathers every shard in its poll loop first and
    // does the patching there; see seastar/include/seastar/core/rendezvous.hh.
    //
    // It is best-effort, because a shard busy with a long task does not reach
    // its poll loop and the phase has a deadline. The result says which
    // happened, and a false is a "try again" rather than a half-done switch.
    hs::set_tracepoints_enabled.set(r, [](std::unique_ptr<request> req) -> future<json::json_return_type> {
        const bool enabled = req->get_query_param("enabled") == "true";
        apilog.info("{} all tracepoints", enabled ? "Enabling" : "Disabling");
        // Shard 0's, and the API server may be on any shard.
        const bool ok = co_await smp::submit_to(0, [enabled] {
            return seastar::set_tracepoints_enabled(enabled);
        });
        if (!ok) {
            apilog.warn("Tracepoint switch gave up: the shards never met at the rendezvous");
        }
        co_return json::json_return_type(ok);
    });

    // Snapshot the binary tracepoint rings of every shard into the workdir.
    //
    // Two phases, and the split is the point. The copy is synchronous on each
    // shard -- seastar::trace_snapshot() returns bytes, it does not stream --
    // so what lands on disk is the ring as it was when the shard was asked,
    // not a ring being written while it is read. The writing afterwards is
    // ordinary blocking I/O in a seastar thread, which is fine for something
    // done once by hand.
    //
    // The decoder source goes in beside the traces: it is generated from the
    // tracepoint tables of *this* binary, which is the only thing that can
    // read them back. See modules/tracer/include/tracer/codegen.h.
    hs::trace_snapshot.set(r, [&ctx, &qp](std::unique_ptr<request> req) -> future<json::json_return_type> {
        const auto now = std::chrono::system_clock::now().time_since_epoch();
        const auto dir = fmt::format("{}/traces/{}", ctx.db.local().get_config().work_directory(),
                std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
        apilog.info("Snapshotting trace buffers into {}", dir);

        const auto decoder = seastar::trace_decoder_source();
        co_await seastar::async([&] {
            std::filesystem::create_directories(dir);
            std::ofstream out(dir + "/decoder.h");
            out << decoder;
        });

        co_await smp::invoke_on_all([&dir, &qp] {
            qp.local().trace_prepared_statements_snapshot();
            auto blob = seastar::trace_snapshot();
            return seastar::async([&dir, blob = std::move(blob)] {
                std::ofstream out(fmt::format("{}/shard-{}.trace", dir, this_shard_id()),
                        std::ios::binary);
                out.write(reinterpret_cast<const char*>(blob.data()), blob.size());
            });
        });

        apilog.info("Trace snapshot written to {}", dir);
        co_return json::json_return_type(sstring(dir));
    });

    hs::drop_sstable_caches.set(r, [&ctx](std::unique_ptr<request> req) {
        apilog.info("Dropping sstable caches");
        return ctx.db.invoke_on_all([] (replica::database& db) {
            return db.drop_caches();
        }).then([] {
            apilog.info("Caches dropped");
            return json::json_return_type(json::json_void());
        });
    });

    hs::dump_profile.set(r, [](std::unique_ptr<request> req) {
        if (!__llvm_profile_dump) {
            apilog.info("Profile will not be dumped, executable is not instrumented with profile dumping.");
            return make_ready_future<json::json_return_type>(json::json_return_type(json::json_void()));
        }
        sstring profile_dest(__llvm_profile_get_filename ? __llvm_profile_get_filename() : "disk");
        apilog.info("Dumping profile to {}", profile_dest);
        __llvm_profile_dump();
        if (__llvm_profile_reset_counters) {
            // If counters are not reset the profile dumping mechanism will issue a warning and exit
            // next time it is attempted. If the counters are reset, profiles can be accumulated
            // (if %m is present in LLVM_PROFILE_FILE pattern) so it can be dumped in stages or
            // multiple times during runtime.
            __llvm_profile_reset_counters();
        } else {
            apilog.warn("Could not reset profile counters, profile dumping will be skipped next time it is attempted");
        }
        apilog.info("Profile dumped to {}", profile_dest);
        return make_ready_future<json::json_return_type>(json::json_return_type(json::json_void()));
    }) ;

    hs::get_highest_supported_sstable_version.set(r, [&ctx] (std::unique_ptr<request> req) {
        return smp::submit_to(0, [&ctx] {
            auto format = ctx.db.local().get_user_sstables_manager().get_highest_supported_format();
            return make_ready_future<json::json_return_type>(seastar::to_sstring(format));
        });
    });

    hs::get_chosen_sstable_version.set(r, [&ctx] (std::unique_ptr<request> req) {
        return smp::submit_to(0, [&ctx] {
            auto format = ctx.db.local().get_user_sstables_manager().get_preferred_sstable_version();
            return make_ready_future<json::json_return_type>(seastar::to_sstring(format));
        });
    });

    hs::get_shard_to_numa_node_mapping.set(r, [](const_req req) {
        auto mapping = local_engine->smp().shard_to_numa_node_mapping();
        return std::vector<unsigned>(mapping.begin(), mapping.end());
    });
}

}
