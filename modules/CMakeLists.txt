# Modules are added bottom-up: add_module() reads its dependencies' stamp
# properties off their targets, so a dependency must already be defined.
add_subdirectory(module_a)
add_subdirectory(module_b)
add_subdirectory(module_c)
add_subdirectory(main)

# Build everything's transitive test markers in one go. module_c's deep stamp
# covers module_a and module_b transitively; main depends on neither, so it is
# named separately.
add_custom_target(test_modules DEPENDS module_c_tested_deep main_tested_deep)
