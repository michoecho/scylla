// The per-frame path: fence wait, acquire, record, submit, present.
//
// This is the logic the port must not change. The ordering and the result
// handling below are the original's, translated from vulkan.hpp to the C API
// and nothing more:
//
//   * Wait on this frame's fence *before* acquiring, with a finite timeout; a
//     timeout abandons the frame rather than blocking forever or treating it as
//     an error.
//   * Read back this frame's screenshot only after that fence has signalled --
//     the fence is what makes the mapped memory safe to read.
//   * Acquire with a finite timeout, and switch on all five interesting
//     results. eErrorOutOfDateKHR abandons the frame; eSuboptimalKHR renders
//     this frame anyway and rebuilds afterwards; eTimeout and eNotReady abandon
//     it; anything else throws.
//   * Reset the fence only once the acquire has succeeded. Resetting before it
//     would leave an unsignalled fence with nothing queued to signal it, which
//     is the classic way to deadlock the next frame on this slot.
//   * Wait on the per-frame acquire semaphore at BLIT, signal the per-image
//     render semaphore at ALL_COMMANDS, and present waiting on that.
//   * Present's eErrorOutOfDateKHR is not an error: the spec still considers
//     the semaphore wait enqueued, so the semaphore is not left signalled.

#include <chrono>
#include <cstdio>

#include "vk_utils.h"
#include "vulkan_module/renderer.h"

namespace vulkan_module {

void Renderer::record_frame(VkCommandBuffer cmd, uint32_t swapchain_image_index) {
    auto& draw = *draw_image_;
    auto& shot = screenshot_buffers_[frame_number_ % FRAME_OVERLAP];

    VkExtent2D draw_extent{draw.extent.width, draw.extent.height};
    VkExtent2D shot_extent{shot.extent.width, shot.extent.height};

    vk_check(vkResetCommandBuffer(cmd, 0), "vkResetCommandBuffer");

    VkCommandBufferBeginInfo begin{};
    begin.sType = VK_STRUCTURE_TYPE_COMMAND_BUFFER_BEGIN_INFO;
    begin.flags = VK_COMMAND_BUFFER_USAGE_ONE_TIME_SUBMIT_BIT;
    vk_check(vkBeginCommandBuffer(cmd, &begin), "vkBeginCommandBuffer");

    // Compute pass: the gradient shader writes the draw image as a storage
    // image, so it must be in GENERAL.
    {
        transition_image(cmd, draw.image, VK_IMAGE_LAYOUT_UNDEFINED, VK_IMAGE_LAYOUT_GENERAL);

        vkCmdBindPipeline(cmd, VK_PIPELINE_BIND_POINT_COMPUTE, gradient_pipeline_);

        VkDescriptorImageInfo img_info{};
        img_info.imageLayout = VK_IMAGE_LAYOUT_GENERAL;
        img_info.imageView = draw.view;

        VkWriteDescriptorSet write{};
        write.sType = VK_STRUCTURE_TYPE_WRITE_DESCRIPTOR_SET;
        write.dstBinding = 0;
        write.descriptorCount = 1;
        write.descriptorType = VK_DESCRIPTOR_TYPE_STORAGE_IMAGE;
        write.pImageInfo = &img_info;
        cmd_push_descriptor_set_(cmd, VK_PIPELINE_BIND_POINT_COMPUTE, gradient_pipeline_layout_, 0, 1, &write);

        // 16x16 threads per group, matching [numthreads] in the shader.
        vkCmdDispatch(cmd, (draw_extent.width + 15) / 16, (draw_extent.height + 15) / 16, 1);
    }

    // Blit the frame onto the acquired swapchain image.
    {
        VkImage swapchain_img = swapchain_images_[swapchain_image_index];
        transition_image(cmd, draw.image, VK_IMAGE_LAYOUT_GENERAL, VK_IMAGE_LAYOUT_TRANSFER_SRC_OPTIMAL);
        transition_image(cmd, swapchain_img, VK_IMAGE_LAYOUT_UNDEFINED, VK_IMAGE_LAYOUT_TRANSFER_DST_OPTIMAL);
        copy_image_to_image(cmd, draw.image, swapchain_img, draw_extent, swapchain_.extent);
        transition_image(
            cmd, swapchain_img, VK_IMAGE_LAYOUT_TRANSFER_DST_OPTIMAL, VK_IMAGE_LAYOUT_PRESENT_SRC_KHR
        );
    }

    // And the same frame, scaled down, into this frame's screenshot buffer.
    // The blit filters linearly, so this is a real downscale rather than a
    // point sample.
    {
        transition_image(cmd, shot.image, VK_IMAGE_LAYOUT_UNDEFINED, VK_IMAGE_LAYOUT_TRANSFER_DST_OPTIMAL);
        copy_image_to_image(cmd, draw.image, shot.image, draw_extent, shot_extent);
        // Make the write visible to the host read that follows the fence.
        transition_image(cmd, shot.image, VK_IMAGE_LAYOUT_TRANSFER_DST_OPTIMAL, VK_IMAGE_LAYOUT_GENERAL);
    }

    vk_check(vkEndCommandBuffer(cmd), "vkEndCommandBuffer");
}

bool Renderer::draw_frame() {
    if (needs_new_swapchain_) {
        recreate_draw_structures();
        needs_new_swapchain_ = false;
    }

    auto& frame = current_frame();

    // Wait for this frame slot's previous submission to complete. A timeout is
    // reported and the frame abandoned -- the fence stays signalled-or-not as
    // it was, so retrying is safe.
    {
        constexpr auto timeout = std::chrono::milliseconds(100);
        VkResult r = vkWaitForFences(
            device_.device, 1, &frame.render_fence, VK_TRUE,
            std::chrono::duration_cast<std::chrono::nanoseconds>(timeout).count()
        );
        switch (r) {
        case VK_SUCCESS:
            break;
        case VK_TIMEOUT:
            std::fprintf(stderr, "[vulkan] vkWaitForFences timed out after %lld ms\n", (long long)timeout.count());
            return false;
        default:
            vk_check(r, "vkWaitForFences");
        }
    }

    // The fence has signalled, so this slot's screenshot blit has landed and
    // its mapped memory is safe to read.
    if (frame.pending_screenshot) {
        latest_screenshot_ = read_screenshot(frame.screenshot_frame);
        frame.pending_screenshot = false;
    }

    uint32_t swapchain_image_index = static_cast<uint32_t>(-1);
    {
        constexpr auto timeout = std::chrono::seconds(2);
        uint32_t idx = static_cast<uint32_t>(-1);
        VkResult result = vkAcquireNextImageKHR(
            device_.device, swapchain_.swapchain,
            std::chrono::duration_cast<std::chrono::nanoseconds>(timeout).count(), frame.swapchain_semaphore,
            VK_NULL_HANDLE, &idx
        );
        switch (result) {
        case VK_ERROR_OUT_OF_DATE_KHR:
            std::fprintf(stderr, "[vulkan] vkAcquireNextImageKHR returned %s\n", result_to_string(result));
            needs_new_swapchain_ = true;
            return false;
        case VK_SUBOPTIMAL_KHR:
            // Still a usable image: render this frame, then rebuild.
            std::fprintf(stderr, "[vulkan] vkAcquireNextImageKHR returned %s\n", result_to_string(result));
            needs_new_swapchain_ = true;
            [[fallthrough]];
        case VK_SUCCESS:
            swapchain_image_index = idx;
            break;
        case VK_TIMEOUT:
            std::fprintf(
                stderr, "[vulkan] vkAcquireNextImageKHR timed out after %lld ms\n",
                (long long)std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count()
            );
            [[fallthrough]];
        case VK_NOT_READY:
            return false;
        default:
            vk_check(result, "vkAcquireNextImageKHR");
        }
        // Only now, with a submission guaranteed to follow, is it safe to
        // unsignal the fence.
        vk_check(vkResetFences(device_.device, 1, &frame.render_fence), "vkResetFences");
    }

    VkCommandBuffer cmd = frame.command_buffer;
    record_frame(cmd, swapchain_image_index);

    {
        VkCommandBufferSubmitInfo cmd_info{};
        cmd_info.sType = VK_STRUCTURE_TYPE_COMMAND_BUFFER_SUBMIT_INFO;
        cmd_info.commandBuffer = cmd;

        // Wait for the presentation engine to be done with the acquired image
        // before the blit that writes it.
        VkSemaphoreSubmitInfo wait_info{};
        wait_info.sType = VK_STRUCTURE_TYPE_SEMAPHORE_SUBMIT_INFO;
        wait_info.semaphore = frame.swapchain_semaphore;
        wait_info.value = 1;
        wait_info.stageMask = VK_PIPELINE_STAGE_2_BLIT_BIT;

        // Signal the semaphore belonging to the image being rendered into, not
        // to the frame slot: present waits on this one.
        VkSemaphoreSubmitInfo signal_info{};
        signal_info.sType = VK_STRUCTURE_TYPE_SEMAPHORE_SUBMIT_INFO;
        signal_info.semaphore = render_semaphores_[swapchain_image_index];
        signal_info.value = 1;
        signal_info.stageMask = VK_PIPELINE_STAGE_2_ALL_COMMANDS_BIT;

        VkSubmitInfo2 submit{};
        submit.sType = VK_STRUCTURE_TYPE_SUBMIT_INFO_2;
        submit.waitSemaphoreInfoCount = 1;
        submit.pWaitSemaphoreInfos = &wait_info;
        submit.commandBufferInfoCount = 1;
        submit.pCommandBufferInfos = &cmd_info;
        submit.signalSemaphoreInfoCount = 1;
        submit.pSignalSemaphoreInfos = &signal_info;

        vk_check(vkQueueSubmit2(graphics_queue_, 1, &submit, frame.render_fence), "vkQueueSubmit2");

        // The screenshot blit is in the submission now; the fence above is what
        // will make it readable.
        frame.pending_screenshot = true;
        frame.screenshot_frame = frame_number_;
    }

    {
        VkPresentInfoKHR present{};
        present.sType = VK_STRUCTURE_TYPE_PRESENT_INFO_KHR;
        present.waitSemaphoreCount = 1;
        present.pWaitSemaphores = &render_semaphores_[swapchain_image_index];
        present.swapchainCount = 1;
        present.pSwapchains = &swapchain_.swapchain;
        present.pImageIndices = &swapchain_image_index;

        VkResult result = vkQueuePresentKHR(graphics_queue_, &present);
        switch (result) {
        case VK_ERROR_OUT_OF_DATE_KHR:
            std::fprintf(stderr, "[vulkan] vkQueuePresentKHR returned %s\n", result_to_string(result));
            needs_new_swapchain_ = true;
            // Spec notes:
            // ```
            // However, if the presentation request is rejected by the presentation engine with an error
            // VK_ERROR_OUT_OF_DATE_KHR, VK_ERROR_FULL_SCREEN_EXCLUSIVE_MODE_LOST_EXT, or
            // VK_ERROR_SURFACE_LOST_KHR, the set of queue operations are still considered to be enqueued and thus
            // any semaphore wait operation specified in VkPresentInfoKHR will execute when the corresponding
            // queue operation is complete.
            // ```
            break;
        case VK_SUBOPTIMAL_KHR:
            std::fprintf(stderr, "[vulkan] vkQueuePresentKHR returned %s\n", result_to_string(result));
            needs_new_swapchain_ = true;
            break;
        case VK_SUCCESS:
            break;
        default:
            vk_check(result, "vkQueuePresentKHR");
            break;
        }
    }

    ++frame_number_;
    ++frames_presented_;
    return true;
}

} // namespace vulkan_module
