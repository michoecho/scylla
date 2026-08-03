# Modules are added bottom-up: add_module() reads its dependencies' stamp
# properties off their targets, so a dependency must already be defined.
add_subdirectory(module_a)
add_subdirectory(module_b)
add_subdirectory(module_c)

# Build everything's transitive test markers in one go.
add_custom_target(test_modules DEPENDS module_c_tested_deep)
