# Depends on both a shared module and a static one, so module_c_test links a
# mixture and module_c_tested_deep is gated on both of their deep stamps.
add_module(module_c
    SOURCES c.cc c_test.cc)

target_link_module(module_c PUBLIC module_a module_b)
