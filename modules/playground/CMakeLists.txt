# Scratch space for experiments that are not part of the program: things worth
# keeping and running in CI, but that nothing else links.
#
# Currently one worked example -- a Hegel property run inside a pt::Trace
# scope, which is what makes hegel::test itself visible in a decoded Intel PT
# trace. That is the point of the combination: exhaustigen supplies a real
# workload with a deep call tree, and pt bounds the region perf records.
add_module(playground
    SOURCES hegel_trace_test.cc)

target_link_module(playground PRIVATE pt exhaustigen)
target_link_libraries(playground PRIVATE hegel::hegel)
