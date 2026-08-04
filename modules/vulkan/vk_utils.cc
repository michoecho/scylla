#include "vk_utils.h"

#include <cstdio>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <unistd.h>

#include "vulkan_module/renderer.h"

namespace vulkan_module {

VkImageSubresourceRange image_subresource_range(VkImageAspectFlags aspect_mask) {
    VkImageSubresourceRange sub_image{};
    sub_image.aspectMask = aspect_mask;
    sub_image.baseMipLevel = 0;
    sub_image.levelCount = VK_REMAINING_MIP_LEVELS;
    sub_image.baseArrayLayer = 0;
    sub_image.layerCount = VK_REMAINING_ARRAY_LAYERS;
    return sub_image;
}

namespace {

const char* layout_to_string(VkImageLayout layout) {
    switch (layout) {
    case VK_IMAGE_LAYOUT_UNDEFINED:
        return "Undefined";
    case VK_IMAGE_LAYOUT_GENERAL:
        return "General";
    case VK_IMAGE_LAYOUT_COLOR_ATTACHMENT_OPTIMAL:
        return "ColorAttachmentOptimal";
    case VK_IMAGE_LAYOUT_DEPTH_ATTACHMENT_OPTIMAL:
        return "DepthAttachmentOptimal";
    case VK_IMAGE_LAYOUT_SHADER_READ_ONLY_OPTIMAL:
        return "ShaderReadOnlyOptimal";
    case VK_IMAGE_LAYOUT_TRANSFER_SRC_OPTIMAL:
        return "TransferSrcOptimal";
    case VK_IMAGE_LAYOUT_TRANSFER_DST_OPTIMAL:
        return "TransferDstOptimal";
    case VK_IMAGE_LAYOUT_PREINITIALIZED:
        return "Preinitialized";
    case VK_IMAGE_LAYOUT_PRESENT_SRC_KHR:
        return "PresentSrcKHR";
    default:
        return "<other>";
    }
}

} // namespace

void transition_image(VkCommandBuffer cmd, VkImage image, VkImageLayout current_layout, VkImageLayout new_layout) {
    VkImageMemoryBarrier2 image_barrier{};
    image_barrier.sType = VK_STRUCTURE_TYPE_IMAGE_MEMORY_BARRIER_2;
    image_barrier.oldLayout = current_layout;
    image_barrier.newLayout = new_layout;
    image_barrier.image = image;
    image_barrier.subresourceRange = image_subresource_range(
        (new_layout == VK_IMAGE_LAYOUT_DEPTH_ATTACHMENT_OPTIMAL) ? VK_IMAGE_ASPECT_DEPTH_BIT
                                                                 : VK_IMAGE_ASPECT_COLOR_BIT
    );

    image_barrier.srcStageMask = VK_PIPELINE_STAGE_2_ALL_COMMANDS_BIT;
    image_barrier.srcAccessMask = VK_ACCESS_2_MEMORY_WRITE_BIT;
    image_barrier.dstStageMask = VK_PIPELINE_STAGE_2_ALL_COMMANDS_BIT;
    image_barrier.dstAccessMask = VK_ACCESS_2_MEMORY_WRITE_BIT | VK_ACCESS_2_MEMORY_READ_BIT;

    if (current_layout == VK_IMAGE_LAYOUT_UNDEFINED && new_layout == VK_IMAGE_LAYOUT_TRANSFER_DST_OPTIMAL) {
        image_barrier.srcAccessMask = 0;
        image_barrier.dstAccessMask = VK_ACCESS_2_TRANSFER_WRITE_BIT;

        image_barrier.srcStageMask = VK_PIPELINE_STAGE_2_BLIT_BIT;
        image_barrier.dstStageMask = VK_PIPELINE_STAGE_2_BLIT_BIT;
    } else if (current_layout == VK_IMAGE_LAYOUT_TRANSFER_DST_OPTIMAL
               && new_layout == VK_IMAGE_LAYOUT_SHADER_READ_ONLY_OPTIMAL) {
        image_barrier.srcAccessMask = VK_ACCESS_2_TRANSFER_WRITE_BIT;
        image_barrier.dstAccessMask = VK_ACCESS_2_SHADER_READ_BIT;

        image_barrier.srcStageMask = VK_PIPELINE_STAGE_2_TRANSFER_BIT;
        image_barrier.dstStageMask = VK_PIPELINE_STAGE_2_FRAGMENT_SHADER_BIT;
    } else if (current_layout == VK_IMAGE_LAYOUT_PREINITIALIZED
               && new_layout == VK_IMAGE_LAYOUT_TRANSFER_SRC_OPTIMAL) {
        image_barrier.srcAccessMask = 0;
        image_barrier.dstAccessMask = VK_ACCESS_2_TRANSFER_READ_BIT;

        image_barrier.srcStageMask = VK_PIPELINE_STAGE_2_HOST_BIT;
        image_barrier.dstStageMask = VK_PIPELINE_STAGE_2_TRANSFER_BIT;
    } else if (current_layout == VK_IMAGE_LAYOUT_UNDEFINED && new_layout == VK_IMAGE_LAYOUT_GENERAL) {
        image_barrier.srcAccessMask = VK_ACCESS_2_TRANSFER_READ_BIT;
        image_barrier.dstAccessMask = VK_ACCESS_2_SHADER_STORAGE_WRITE_BIT;

        image_barrier.srcStageMask = VK_PIPELINE_STAGE_2_BLIT_BIT;
        image_barrier.dstStageMask = VK_PIPELINE_STAGE_2_COMPUTE_SHADER_BIT;
    } else if (current_layout == VK_IMAGE_LAYOUT_GENERAL
               && new_layout == VK_IMAGE_LAYOUT_COLOR_ATTACHMENT_OPTIMAL) {
        image_barrier.srcAccessMask = VK_ACCESS_2_SHADER_STORAGE_WRITE_BIT;
        image_barrier.dstAccessMask = VK_ACCESS_2_COLOR_ATTACHMENT_READ_BIT;

        image_barrier.srcStageMask = VK_PIPELINE_STAGE_2_COMPUTE_SHADER_BIT;
        image_barrier.dstStageMask = VK_PIPELINE_STAGE_2_COLOR_ATTACHMENT_OUTPUT_BIT;
    } else if (current_layout == VK_IMAGE_LAYOUT_COLOR_ATTACHMENT_OPTIMAL
               && new_layout == VK_IMAGE_LAYOUT_GENERAL) {
        image_barrier.srcAccessMask = 0;
        image_barrier.dstAccessMask = VK_ACCESS_2_SHADER_STORAGE_WRITE_BIT;

        image_barrier.srcStageMask = VK_PIPELINE_STAGE_2_TOP_OF_PIPE_BIT;
        image_barrier.dstStageMask = VK_PIPELINE_STAGE_2_COMPUTE_SHADER_BIT;
    } else if (current_layout == VK_IMAGE_LAYOUT_COLOR_ATTACHMENT_OPTIMAL
               && new_layout == VK_IMAGE_LAYOUT_TRANSFER_SRC_OPTIMAL) {
        image_barrier.srcAccessMask = VK_ACCESS_2_COLOR_ATTACHMENT_WRITE_BIT;
        image_barrier.dstAccessMask = VK_ACCESS_2_TRANSFER_READ_BIT;

        image_barrier.srcStageMask = VK_PIPELINE_STAGE_2_COLOR_ATTACHMENT_OUTPUT_BIT;
        image_barrier.dstStageMask = VK_PIPELINE_STAGE_2_TRANSFER_BIT;
    } else if (current_layout == VK_IMAGE_LAYOUT_TRANSFER_DST_OPTIMAL
               && new_layout == VK_IMAGE_LAYOUT_PRESENT_SRC_KHR) {
        image_barrier.srcAccessMask = VK_ACCESS_2_TRANSFER_WRITE_BIT;
        image_barrier.dstAccessMask = 0;

        image_barrier.srcStageMask = VK_PIPELINE_STAGE_2_BLIT_BIT;
        image_barrier.dstStageMask = 0;
    } else if (current_layout == VK_IMAGE_LAYOUT_GENERAL && new_layout == VK_IMAGE_LAYOUT_TRANSFER_SRC_OPTIMAL) {
        // The draw image is read straight from the compute shader's output
        // rather than going through a color attachment pass first -- the port
        // has no ImGui pass between the two.
        image_barrier.srcAccessMask = VK_ACCESS_2_SHADER_STORAGE_WRITE_BIT;
        image_barrier.dstAccessMask = VK_ACCESS_2_TRANSFER_READ_BIT;

        image_barrier.srcStageMask = VK_PIPELINE_STAGE_2_COMPUTE_SHADER_BIT;
        image_barrier.dstStageMask = VK_PIPELINE_STAGE_2_TRANSFER_BIT;
    } else if (current_layout == VK_IMAGE_LAYOUT_TRANSFER_DST_OPTIMAL
               && new_layout == VK_IMAGE_LAYOUT_GENERAL) {
        // Screenshot buffer, read back by the host after the blit into it.
        image_barrier.srcAccessMask = VK_ACCESS_2_TRANSFER_WRITE_BIT;
        image_barrier.dstAccessMask = VK_ACCESS_2_HOST_READ_BIT;

        image_barrier.srcStageMask = VK_PIPELINE_STAGE_2_BLIT_BIT;
        image_barrier.dstStageMask = VK_PIPELINE_STAGE_2_HOST_BIT;
    } else {
        throw std::invalid_argument(
            std::string("unsupported layout transition ") + layout_to_string(current_layout) + " -> "
            + layout_to_string(new_layout) + "!"
        );
    }

    VkDependencyInfo dep{};
    dep.sType = VK_STRUCTURE_TYPE_DEPENDENCY_INFO;
    dep.imageMemoryBarrierCount = 1;
    dep.pImageMemoryBarriers = &image_barrier;
    vkCmdPipelineBarrier2(cmd, &dep);
}

void copy_image_to_image(
    VkCommandBuffer cmd, VkImage source, VkImage destination, VkExtent2D src_size, VkExtent2D dst_size
) {
    VkImageBlit2 blit_region{.sType = VK_STRUCTURE_TYPE_IMAGE_BLIT_2, .pNext = nullptr};

    blit_region.srcOffsets[1].x = src_size.width;
    blit_region.srcOffsets[1].y = src_size.height;
    blit_region.srcOffsets[1].z = 1;

    blit_region.dstOffsets[1].x = dst_size.width;
    blit_region.dstOffsets[1].y = dst_size.height;
    blit_region.dstOffsets[1].z = 1;

    blit_region.srcSubresource.aspectMask = VK_IMAGE_ASPECT_COLOR_BIT;
    blit_region.srcSubresource.baseArrayLayer = 0;
    blit_region.srcSubresource.layerCount = 1;
    blit_region.srcSubresource.mipLevel = 0;

    blit_region.dstSubresource.aspectMask = VK_IMAGE_ASPECT_COLOR_BIT;
    blit_region.dstSubresource.baseArrayLayer = 0;
    blit_region.dstSubresource.layerCount = 1;
    blit_region.dstSubresource.mipLevel = 0;

    VkBlitImageInfo2 blit_info{.sType = VK_STRUCTURE_TYPE_BLIT_IMAGE_INFO_2, .pNext = nullptr};
    blit_info.dstImage = destination;
    blit_info.dstImageLayout = VK_IMAGE_LAYOUT_TRANSFER_DST_OPTIMAL;
    blit_info.srcImage = source;
    blit_info.srcImageLayout = VK_IMAGE_LAYOUT_TRANSFER_SRC_OPTIMAL;
    blit_info.filter = VK_FILTER_LINEAR;
    blit_info.regionCount = 1;
    blit_info.pRegions = &blit_region;

    vkCmdBlitImage2(cmd, &blit_info);
}

VkImageCreateInfo image_create_info(VkFormat format, VkImageUsageFlags usage_flags, VkExtent3D extent) {
    VkImageCreateInfo info{};
    info.sType = VK_STRUCTURE_TYPE_IMAGE_CREATE_INFO;
    info.imageType = VK_IMAGE_TYPE_2D;
    info.format = format;
    info.extent = extent;
    info.mipLevels = 1;
    info.arrayLayers = 1;
    // For MSAA; unused here, so one sample per pixel.
    info.samples = VK_SAMPLE_COUNT_1_BIT;
    // Optimal tiling: the image is stored in whatever layout the GPU prefers.
    info.tiling = VK_IMAGE_TILING_OPTIMAL;
    info.usage = usage_flags;
    return info;
}

VkImageViewCreateInfo imageview_create_info(VkFormat format, VkImage image, VkImageAspectFlags aspect_flags) {
    VkImageViewCreateInfo info{};
    info.sType = VK_STRUCTURE_TYPE_IMAGE_VIEW_CREATE_INFO;
    info.image = image;
    info.viewType = VK_IMAGE_VIEW_TYPE_2D;
    info.format = format;
    info.subresourceRange.aspectMask = aspect_flags;
    info.subresourceRange.baseMipLevel = 0;
    info.subresourceRange.levelCount = 1;
    info.subresourceRange.baseArrayLayer = 0;
    info.subresourceRange.layerCount = 1;
    return info;
}

VkShaderModule load_shader_module(const std::filesystem::path& path, VkDevice device) {
    std::ifstream file(path, std::ios::binary | std::ios::ate);
    if (!file) {
        throw std::runtime_error("failed to open shader " + path.string());
    }
    const auto size = static_cast<std::streamsize>(file.tellg());
    if (size <= 0 || size % sizeof(uint32_t) != 0) {
        throw std::runtime_error("shader " + path.string() + " is not a whole number of SPIR-V words");
    }
    std::vector<uint32_t> buffer(static_cast<size_t>(size) / sizeof(uint32_t));
    file.seekg(0);
    file.read(reinterpret_cast<char*>(buffer.data()), size);
    if (!file) {
        throw std::runtime_error("failed to read shader " + path.string());
    }

    VkShaderModuleCreateInfo create_info{};
    create_info.sType = VK_STRUCTURE_TYPE_SHADER_MODULE_CREATE_INFO;
    create_info.codeSize = buffer.size() * sizeof(uint32_t);
    create_info.pCode = buffer.data();

    VkShaderModule module = VK_NULL_HANDLE;
    vk_check(vkCreateShaderModule(device, &create_info, nullptr, &module), "vkCreateShaderModule");
    return module;
}

std::filesystem::path executable_directory() {
    std::error_code ec;
    auto exe = std::filesystem::read_symlink("/proc/self/exe", ec);
    if (ec) {
        throw std::runtime_error("cannot resolve /proc/self/exe: " + ec.message());
    }
    return exe.parent_path();
}

} // namespace vulkan_module
