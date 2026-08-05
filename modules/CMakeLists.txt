# Order is irrelevant: add_module() reads nothing off its dependencies, and
# target_link_module() only requires that the dependency exists by the time it
# is called.
add_subdirectory(snapshot)
add_subdirectory(exhaustigen)
add_subdirectory(module_a)
add_subdirectory(module_b)
add_subdirectory(module_c)
add_subdirectory(vulkan)
add_subdirectory(pt)
add_subdirectory(main)
