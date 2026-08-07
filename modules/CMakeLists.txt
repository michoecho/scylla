# add_module() reads nothing off its dependencies, but target_link_module()
# requires the dependency to already be a target when it is called -- so a
# module must be added before anything that links it. Hence the two blocks:
# the shared test infrastructure first, its consumers after.
add_subdirectory(snapshot)
add_subdirectory(exhaustigen)
add_subdirectory(test_rng)

add_subdirectory(exponential_histogram)
add_subdirectory(vulkan)
add_subdirectory(pt)
add_subdirectory(exception_hacks)
add_subdirectory(playground)
add_subdirectory(main)
