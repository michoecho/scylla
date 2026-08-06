# Snapshot ("expect") tests: assertions whose expected value is rewritten into
# the source by the test runner. See include/snapshot/snapshot.h, and
# include/snapshot/check.h for the assertion itself.
#
# Depends on no other module: this is infrastructure that any module may use, so
# it sits at the bottom of the graph.
add_module(snapshot
    SOURCES
        snapshot.cc
        updater.cc
        # Applies recorded updates once the run has finished. A doctest
        # listener, so linking this module is all it takes to get the behaviour
        # -- no change to the shared runner.
        flush_reporter.cc
        # Ordinary unit tests, not snapshot tests -- a snapshot test of the
        # updater or of compare() would rewrite its own assertions when that
        # code broke. See the comment at the top of each file.
        updater_test.cc
        compare_test.cc
        filesnap_test.cc
        # The worked example, which is also what a reader should copy from.
        example_test.cc)

target_compile_options(snapshot PRIVATE -Wall -Wextra)
target_compile_definitions(snapshot PRIVATE
    SNAPSHOT_ROOT="${PROJECT_SOURCE_DIR}/.snapshots")
