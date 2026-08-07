add_module(exponential_histogram
    SOURCES
        exponential_histogram.cc
        integer_to_float.cc)

target_link_module(exponential_histogram PRIVATE snapshot)

# Randomized tests go through the test_rng facade rather than straight at
# Hegel, so the engine driving them is TEST_RNG's choice rather than this
# module's. Hegel is one backend behind it and is not a dependency here.
target_link_module(exponential_histogram PRIVATE test_rng)
