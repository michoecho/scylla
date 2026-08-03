# Depends on both a shared module and a static one, so module_c_test links a
# mixture and module_c_tested_deep is gated on both of their deep stamps.
add_module(module_c
    SOURCES c.cc
    TEST_SOURCES c_test.cc
    DEPS module_a module_b)
