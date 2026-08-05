add_module(exponential_histogram
    SOURCES
        exponential_histogram.cc
        integer_to_float.cc)

target_link_module(exponential_histogram PRIVATE snapshot)
target_link_libraries(exponential_histogram PRIVATE hegel::hegel)
