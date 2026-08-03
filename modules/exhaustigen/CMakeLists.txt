# Exhaustive sequence generation, adapted from graydon/exhaustigen.
# Header-only: Gen is a small class and half of it is templates, so there is
# nothing to compile into the library but the tests themselves.
#
# Depends on snapshot, because its tests are snapshot tests -- see
# exhaustigen_test.cc.
add_module(exhaustigen
    SOURCES exhaustigen_test.cc)

target_link_module(exhaustigen PRIVATE snapshot)
