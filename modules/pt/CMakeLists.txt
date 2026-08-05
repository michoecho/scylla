# Program side of the Intel PT control protocol driven by tools/pt-trace: the
# pt::enable()/pt::disable()/pt::Trace helpers that bound a trace to a lexical
# region, and the worked example the orchestrator's own tests decode.
#
# A module of its own rather than part of `main` because nothing in the program
# uses it -- it is opt-in instrumentation that any module can link when it wants
# a traced region, and it depends on nothing but libc.
add_module(pt
    SOURCES
        pt_control.cc
        # The noinline call tree (pt_outer -> pt_middle -> pt_leaf) that
        # tools/tests/test_pt_trace.py looks for in the decoded trace, plus the
        # test that wraps it in a pt::Trace scope. Runs untraced -- and passes --
        # when the control fifos are absent.
        pt_control_test.cc)

target_compile_options(pt PRIVATE
    -Wall
    -Wextra
)
