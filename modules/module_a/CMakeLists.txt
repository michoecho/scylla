# Shared library module, with no dependencies of its own: the bottom of the
# graph, and the case that exercises doctest's cross-shared-object registry.
add_module(module_a
    TYPE SHARED
    SOURCES a.cc
    TEST_SOURCES a_test.cc)
