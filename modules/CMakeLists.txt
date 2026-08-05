# Order is irrelevant: add_module() reads nothing off its dependencies, and
# target_link_module() only requires that the dependency exists by the time it
# is called.
add_subdirectory(snapshot)
add_subdirectory(exponential_histogram)
add_subdirectory(exhaustigen)
add_subdirectory(vulkan)
add_subdirectory(pt)
add_subdirectory(exception_hacks)
add_subdirectory(playground)
add_subdirectory(main)
