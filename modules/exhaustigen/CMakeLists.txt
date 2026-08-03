# Exhaustive sequence generation, adapted from graydon/exhaustigen and used
# under the Apache License 2.0 -- see LICENSE beside this file, and the notice
# at the top of include/exhaustigen/exhaustigen.h.
#
# Header-only: Gen is a small class and half of it is templates, so there is
# nothing to compile into the library but the tests themselves.
#
# Depends on snapshot, because its tests are snapshot tests -- see
# exhaustigen_test.cc.
add_module(exhaustigen
    SOURCES exhaustigen_test.cc)

target_link_module(exhaustigen PRIVATE snapshot)
