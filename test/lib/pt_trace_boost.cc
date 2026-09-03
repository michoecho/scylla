/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/tree/observer.hpp>
#include <boost/test/unit_test.hpp>

#include "../../../../modules/pt/include/pt/pt_trace.h"

namespace {

class pt_trace_observer final : public boost::unit_test::test_observer {
public:
    void test_start(boost::unit_test::counter_t, boost::unit_test::test_unit_id) override {
        _trace = pt::perf_trace::start_if_requested();
    }

    void test_finish() override {
        _trace.reset();
    }

    void test_aborted() override {
        _trace.reset();
    }

private:
    std::unique_ptr<pt::perf_trace> _trace;
};

pt_trace_observer observer;

struct observer_registration {
    observer_registration() {
        boost::unit_test::framework::register_observer(observer);
    }
};

observer_registration registration;

} // namespace
