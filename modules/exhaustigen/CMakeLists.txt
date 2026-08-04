# Exhaustive sequence generation, adapted from graydon/exhaustigen and used
# under the Apache License 2.0 -- see LICENSE beside this file, and the notice
# at the top of include/exhaustigen/exhaustigen.h.
#
# Header-only: Gen is a small class and half of it is templates, so there is
# nothing to compile into the library but the tests themselves.
#
# Depends on snapshot for the snapshot tests in exhaustigen_test.cc, and on
# hegel for the property-based tests in hegel_splits_test.cc.
add_module(exhaustigen
    SOURCES
        exhaustigen_test.cc
        # Property-based tests for gen_splits, whose interesting behaviour is
        # over a space of arguments too large to snapshot.
        hegel_splits_test.cc)

target_link_module(exhaustigen PRIVATE snapshot)
target_link_libraries(exhaustigen PRIVATE hegel::hegel)
