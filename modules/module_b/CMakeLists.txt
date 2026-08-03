# Static library module. The counterpart to module_a: this is the case where
# --whole-archive is what keeps the test registrations from being dropped.
add_module(module_b
    TYPE STATIC
    SOURCES b.cc b_test.cc)
