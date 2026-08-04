#pragma once

// Free helpers over the Vulkan C API: barriers, blits, shader loading.
//
// transition_image() is a port of the original's layout-transition table and
// keeps its exact structure -- an explicit case per (old, new) layout pair with
// hand-picked stage and access masks, and a throw on any pair not in the table.
// That deliberateness is the point: an unlisted transition is a bug to be
// noticed, not something to paper over with ALL_COMMANDS/MEMORY_READ|WRITE.
//
// Private to the module: this file is not under include/, so only the vulkan
// module can include it.

#include <cstdint>
#include <filesystem>

#include <vulkan/vulkan.h>

namespace vulkan_module {

VkImageSubresourceRange image_subresource_range(VkImageAspectFlags aspect_mask);

// Inserts a barrier moving `image` from current_layout to new_layout.
// Throws std::invalid_argument for a pair the table does not cover.
void transition_image(VkCommandBuffer cmd, VkImage image, VkImageLayout current_layout, VkImageLayout new_layout);

// Blits the whole of `source` onto the whole of `destination`, scaling with a
// linear filter. This is what downscales a frame into a screenshot buffer.
void copy_image_to_image(
    VkCommandBuffer cmd, VkImage source, VkImage destination, VkExtent2D src_size, VkExtent2D dst_size
);

VkImageCreateInfo image_create_info(VkFormat format, VkImageUsageFlags usage_flags, VkExtent3D extent);

VkImageViewCreateInfo imageview_create_info(VkFormat format, VkImage image, VkImageAspectFlags aspect_flags);

// Reads a SPIR-V file and creates a shader module from it.
VkShaderModule load_shader_module(const std::filesystem::path& path, VkDevice device);

// Directory holding this executable, used to locate compiled shaders next to
// the binary rather than relative to the working directory.
std::filesystem::path executable_directory();

} // namespace vulkan_module
