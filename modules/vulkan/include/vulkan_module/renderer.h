#pragma once

// An SDL3 window with a Vulkan renderer behind it, as one class.
//
// The Vulkan C API is used throughout -- no vulkan.hpp. Handles are owned by
// Renderer and destroyed in its destructor, so the type is move-only and
// non-copyable rather than wrapping each handle in its own RAII shell.
//
// The swapchain, the per-frame synchronization, and the error handling are
// ports of a working program and their logic is deliberately unchanged: the
// same fence-then-acquire order, the same per-frame and per-swapchain-image
// semaphores, the same treatment of eErrorOutOfDateKHR / eSuboptimalKHR /
// eTimeout at each of the three points they can surface (acquire, submit,
// present). Only the spelling moved from vulkan.hpp to the C API.

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include <SDL3/SDL.h>
#include <SDL3/SDL_vulkan.h>
#include <VkBootstrap.h>
#include <vk_mem_alloc.h>
#include <vulkan/vulkan.h>

namespace vulkan_module {

// Thrown by vk_check() and by the builder wrappers. Carries the VkResult so a
// caller can distinguish "no usable device" from a genuine failure -- the test
// uses this to skip rather than fail on a machine with no GPU.
struct vulkan_error : std::runtime_error {
    VkResult result;
    vulkan_error(VkResult result, const std::string& what)
        : std::runtime_error(what)
        , result(result) {
    }
};

// Thrown when SDL fails; carries SDL_GetError()'s text.
struct sdl_error : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// Throws vulkan_error unless r is VK_SUCCESS. Every Vulkan call whose result is
// not explicitly switched on goes through this.
void vk_check(VkResult r, const char* what = "vk_check");

// Throws sdl_error if the SDL call reported failure.
void sdl_check(bool ok, const char* what = "sdl_check");

const char* result_to_string(VkResult r);

// A host-visible, linear-tiled RGBA8 image holding one downscaled frame, read
// back into ordinary memory. Row-major, tightly packed: `width * 4` bytes per
// row, no padding -- the row pitch the GPU chose is removed during readback.
struct Screenshot {
    uint32_t width = 0;
    uint32_t height = 0;
    std::vector<uint8_t> pixels; // RGBA8, width*height*4 bytes

    struct Rgba {
        uint8_t r, g, b, a;
    };

    Rgba at(uint32_t x, uint32_t y) const;
};

// A GPU image plus the allocation backing it.
struct AllocatedImage {
    VkImage image = VK_NULL_HANDLE;
    VkImageView view = VK_NULL_HANDLE;
    VmaAllocation allocation = nullptr;
    VmaAllocationInfo allocation_info = {};
    VkExtent3D extent = {};
    VkFormat format = VK_FORMAT_UNDEFINED;
};

// How to build the window and renderer. At namespace scope rather than nested
// in Renderer so that `Renderer(Config = {})` can use these initializers -- a
// nested class's defaults are not complete until the enclosing class is.
struct RendererConfig {
    const char* title = "cpp_template";
    int width = 800;
    int height = 600;
    // Off for tests: a window that never maps still presents, and the
    // swapchain path being exercised is the same either way.
    bool visible = true;
    bool validation = true;
    // Longest downscaled screenshot edge. Preserves aspect ratio; the
    // other edge is rounded up to a multiple of 4.
    uint32_t screenshot_max_dim = 480;
};

class Renderer {
public:
    using Config = RendererConfig;

    explicit Renderer(Config config = {});
    ~Renderer();

    Renderer(const Renderer&) = delete;
    Renderer& operator=(const Renderer&) = delete;
    Renderer(Renderer&&) = delete;
    Renderer& operator=(Renderer&&) = delete;

    // Draws one frame: the gradient compute shader into the draw image, then a
    // blit to the acquired swapchain image and another to this frame's
    // screenshot buffer, then submit and present.
    //
    // Returns false when the frame was abandoned before any work was submitted
    // -- swapchain out of date, acquire timed out, fence wait timed out. That
    // is a normal outcome, not an error; call again.
    bool draw_frame();

    // Pumps SDL's event queue. Returns false once the window has been asked to
    // close.
    bool poll_events();

    // The screenshot of the most recently *completed* frame, or nullopt when no
    // frame has finished yet. Reading is CPU-side only: the frame's fence has
    // already been waited on by the time its buffer is read, so this never
    // stalls on the GPU.
    std::optional<Screenshot> take_screenshot();

    // Blocks until every in-flight frame has completed and returns the newest
    // available screenshot. This is the convenient form for a test: draw a few
    // frames, then ask what was on screen.
    std::optional<Screenshot> take_screenshot_blocking();

    // Number of frames whose submission completed.
    uint64_t frames_presented() const {
        return frames_presented_;
    }

    const std::string& device_name() const {
        return physical_device_.name;
    }

    VkExtent2D swapchain_extent() const {
        return swapchain_.extent;
    }

private:
    // Frames in flight. Two, so the CPU can record frame N+1 while the GPU is
    // still working on frame N.
    static constexpr unsigned FRAME_OVERLAP = 2;

    // Everything owned per in-flight frame. The fence is what the CPU waits on
    // before reusing any of it; the semaphore is signalled by the presentation
    // engine when the acquired image is ready to be written.
    struct FrameData {
        VkCommandPool command_pool = VK_NULL_HANDLE;
        VkCommandBuffer command_buffer = VK_NULL_HANDLE;
        VkFence render_fence = VK_NULL_HANDLE;
        VkSemaphore swapchain_semaphore = VK_NULL_HANDLE;
        // Set once this frame's screenshot blit has been submitted; cleared
        // when the buffer is read back after the fence signals.
        bool pending_screenshot = false;
        // Which frame number the pending screenshot belongs to.
        uint64_t screenshot_frame = 0;
    };

    // Releases every handle this object owns, in dependency order, tolerating
    // nulls. Called by the destructor and by the constructor's failure path --
    // the latter cannot invoke the destructor itself, since destroying an
    // object whose construction never completed would double-destroy the
    // members that did get built.
    void teardown() noexcept;

    void init_window();
    void init_instance();
    void init_device();
    void init_allocator();
    void init_commands();
    void init_descriptors();
    void init_pipeline();

    void create_swapchain();
    void destroy_swapchain();
    // waitIdle, then rebuild the swapchain and everything sized from it.
    void recreate_draw_structures();

    void create_swapchain_image_data();
    void destroy_swapchain_image_data();
    void create_draw_image();
    void destroy_draw_image();
    void create_screenshot_buffers();
    void destroy_screenshot_buffers();

    void record_frame(VkCommandBuffer cmd, uint32_t swapchain_image_index);
    // Copies a completed frame's screenshot buffer out of mapped memory,
    // undoing the image's row pitch.
    Screenshot read_screenshot(uint64_t frame);

    AllocatedImage allocate_image(
        VkFormat format, VkImageUsageFlags usage, VkExtent3D extent, const VmaAllocationCreateInfo& alloc_info,
        VkImageTiling tiling = VK_IMAGE_TILING_OPTIMAL, bool with_view = true
    );
    void destroy_image(AllocatedImage& img);

    FrameData& current_frame() {
        return frames_[frame_number_ % FRAME_OVERLAP];
    }

    Config config_;

    SDL_Window* window_ = nullptr;
    bool sdl_initialized_ = false;
    bool should_quit_ = false;

    vkb::Instance instance_;
    vkb::PhysicalDevice physical_device_;
    vkb::Device device_;
    vkb::Swapchain swapchain_;
    bool has_swapchain_ = false;

    VkSurfaceKHR surface_ = VK_NULL_HANDLE;
    VkQueue graphics_queue_ = VK_NULL_HANDLE;
    uint32_t graphics_queue_family_ = 0;

    VmaAllocator allocator_ = nullptr;

    std::vector<VkImage> swapchain_images_;
    // One semaphore per swapchain image, not per frame in flight: it is
    // signalled by the submit that renders into that image and waited on by the
    // present of that same image, so its lifetime is tied to the image.
    std::vector<VkSemaphore> render_semaphores_;

    std::vector<FrameData> frames_;

    // Rendered into at swapchain resolution, then blitted down to both the
    // swapchain image and the screenshot buffer.
    std::optional<AllocatedImage> draw_image_;
    // One per frame in flight, host-visible and permanently mapped.
    std::vector<AllocatedImage> screenshot_buffers_;
    // Set by read_screenshot; the newest fully read-back frame.
    std::optional<Screenshot> latest_screenshot_;

    // Extension entry point: the loader exports only core symbols, so this is
    // resolved from the device once and called through the pointer.
    PFN_vkCmdPushDescriptorSetKHR cmd_push_descriptor_set_ = nullptr;

    VkDescriptorSetLayout draw_image_descriptor_layout_ = VK_NULL_HANDLE;
    VkPipelineLayout gradient_pipeline_layout_ = VK_NULL_HANDLE;
    VkPipeline gradient_pipeline_ = VK_NULL_HANDLE;

    uint64_t frame_number_ = 0;
    uint64_t frames_presented_ = 0;
    bool needs_new_swapchain_ = true;
};

} // namespace vulkan_module
