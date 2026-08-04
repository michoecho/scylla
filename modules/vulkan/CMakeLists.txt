# SDL3 + Vulkan, encapsulated in one class (vulkan_module::Renderer).
#
# The Vulkan C API is used throughout -- no vulkan.hpp -- so the only headers
# involved are vulkan/vulkan.h plus vk-bootstrap and VMA, all from nixpkgs via
# the devShell. Nothing here is vendored or a submodule.
add_module(vulkan
    SOURCES
        renderer.cc
        # The frame loop: fence wait, acquire, record, submit, present. Kept in
        # its own TU because it is the ported logic that must not drift -- see
        # the header comment there.
        frame.cc
        vk_utils.cc
        vulkan_test.cc)

target_link_libraries(vulkan PUBLIC
    Vulkan::Vulkan
    SDL3::SDL3
    vk-bootstrap::vk-bootstrap
    GPUOpen::VulkanMemoryAllocator)

# VMA is a header-only library that needs exactly one TU to instantiate its
# implementation; renderer.cc is not it, so the definition goes on a dedicated
# source rather than leaking VMA_IMPLEMENTATION into every TU.
target_sources(vulkan PRIVATE vma_impl.cc)

target_compile_options(vulkan PRIVATE -Wall -Wextra)

# VMA's headers trip several warnings under -Wall -Wextra that are not ours to
# fix, and vk-bootstrap's designated-initializer use trips another. Scoped to
# this module.
target_compile_options(vulkan PRIVATE
    -Wno-nullability-completeness
    -Wno-missing-field-initializers)

# Shaders: slangc compiles each .slang to SPIR-V beside the test executable, so
# the renderer can find them relative to /proc/self/exe rather than depending on
# the working directory a test happens to run in.
set(VULKAN_SHADER_DIR "${CMAKE_CURRENT_BINARY_DIR}/shaders")
file(MAKE_DIRECTORY "${VULKAN_SHADER_DIR}")

set(VULKAN_SPIRV_FILES)
file(GLOB VULKAN_SLANG_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/shaders/*.slang")
foreach(SLANG ${VULKAN_SLANG_SOURCES})
    get_filename_component(SLANG_NAME ${SLANG} NAME)
    set(SPIRV "${VULKAN_SHADER_DIR}/${SLANG_NAME}.spv")
    add_custom_command(
        OUTPUT ${SPIRV}
        COMMAND ${SLANGC} ${SLANG} -target spirv -o ${SPIRV}
        DEPENDS ${SLANG}
        COMMENT "slangc ${SLANG_NAME}")
    list(APPEND VULKAN_SPIRV_FILES ${SPIRV})
endforeach()

add_custom_target(vulkan_shaders DEPENDS ${VULKAN_SPIRV_FILES})

# The test binary loads them at run time, so they must exist before it runs --
# and the stamp target that runs the suite is gated on the test executable, so
# depending here is enough.
add_dependencies(vulkan_test vulkan_shaders)
