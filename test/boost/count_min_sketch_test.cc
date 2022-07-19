/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE core
#include "utils/count_min_sketch.hh"
#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(basic) {
    utils::count_min_sketch cms(100);
    cms.increment(5);
    cms.increment(6);
    cms.increment(6);
    BOOST_CHECK_EQUAL(cms.estimate(4), 0);
    BOOST_CHECK_EQUAL(cms.estimate(5), 1);
    BOOST_CHECK_EQUAL(cms.estimate(6), 2);
    cms.halve();
    BOOST_CHECK_EQUAL(cms.estimate(4), 0);
    BOOST_CHECK_EQUAL(cms.estimate(5), 0);
    BOOST_CHECK_EQUAL(cms.estimate(6), 1);
}
