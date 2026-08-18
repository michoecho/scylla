#include "vulkan_module/renderer.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <system_error>

#include "vk_utils.h"

namespace vulkan_module {

namespace {

// Rounds a up to the next multiple of b.
uint32_t round_up(uint32_t a, uint32_t b) {
    uint32_t x = a + b - 1;
    return x - x % b;
}

// The validation layer's callback. Errors and warnings go to stderr; anything
// quieter is dropped, so a passing test stays silent.
VKAPI_ATTR VkBool32 VKAPI_CALL debug_callback(
    VkDebugUtilsMessageSeverityFlagBitsEXT severity, VkDebugUtilsMessageTypeFlagsEXT /*types*/,
    const VkDebugUtilsMessengerCallbackDataEXT* data, void* /*user_data*/
) {
    if (severity & (VK_DEBUG_UTILS_MESSAGE_SEVERITY_ERROR_BIT_EXT | VK_DEBUG_UTILS_MESSAGE_SEVERITY_WARNING_BIT_EXT)) {
        std::fprintf(
            stderr, "[vulkan] %s: %s\n",
            (severity & VK_DEBUG_UTILS_MESSAGE_SEVERITY_ERROR_BIT_EXT) ? "error" : "warning",
            data->pMessage ? data->pMessage : "<no message>"
        );
    }
    return VK_FALSE;
}

} // namespace

const char* result_to_string(VkResult r) {
    switch (r) {
    case VK_SUCCESS:
        return "eSuccess";
    case VK_NOT_READY:
        return "eNotReady";
    case VK_TIMEOUT:
        return "eTimeout";
    case VK_SUBOPTIMAL_KHR:
        return "eSuboptimalKHR";
    case VK_ERROR_OUT_OF_DATE_KHR:
        return "eErrorOutOfDateKHR";
    case VK_ERROR_SURFACE_LOST_KHR:
        return "eErrorSurfaceLostKHR";
    case VK_ERROR_DEVICE_LOST:
        return "eErrorDeviceLost";
    case VK_ERROR_OUT_OF_HOST_MEMORY:
        return "eErrorOutOfHostMemory";
    case VK_ERROR_OUT_OF_DEVICE_MEMORY:
        return "eErrorOutOfDeviceMemory";
    case VK_ERROR_INITIALIZATION_FAILED:
        return "eErrorInitializationFailed";
    case VK_ERROR_EXTENSION_NOT_PRESENT:
        return "eErrorExtensionNotPresent";
    case VK_ERROR_FEATURE_NOT_PRESENT:
        return "eErrorFeatureNotPresent";
    case VK_ERROR_INCOMPATIBLE_DRIVER:
        return "eErrorIncompatibleDriver";
    default:
        return "<unknown VkResult>";
    }
}

void vk_check(VkResult r, const char* what) {
    if (r != VK_SUCCESS) {
        throw vulkan_error(r, std::string(what) + ": " + result_to_string(r));
    }
}

void sdl_check(bool ok, const char* what) {
    if (!ok) {
        const char* err = SDL_GetError();
        throw sdl_error(std::string(what) + ": " + (err && *err ? err : "<no SDL error>"));
    }
}

Screenshot::Rgba Screenshot::at(uint32_t x, uint32_t y) const {
    const size_t offset = (static_cast<size_t>(y) * width + x) * 4;
    return Rgba{pixels[offset], pixels[offset + 1], pixels[offset + 2], pixels[offset + 3]};
}

Renderer::Renderer(Config config)
    : config_(config) {
    // Ordered: the window must exist before the surface, the surface before
    // device selection (presentation support is part of the choice), and the
    // device before anything allocated from it. Anything that throws part-way
    // leaves the destructor to clean up what was built, hence the handle
    // members all being null-initialized.
    try {
        init_window();
        init_instance();
        init_device();
        init_allocator();
        init_commands();
        init_descriptors();
        init_pipeline();
        recreate_draw_structures();
        needs_new_swapchain_ = false;
    } catch (...) {
        // The destructor is not run for an object whose constructor threw, so
        // release here. Every teardown step tolerates null handles.
        teardown();
        throw;
    }
}

void Renderer::init_window() {
    sdl_check(SDL_Init(SDL_INIT_VIDEO), "SDL_Init");
    sdl_initialized_ = true;

    SDL_PropertiesID props = SDL_CreateProperties();
    sdl_check(props != 0, "SDL_CreateProperties");
    SDL_SetBooleanProperty(props, SDL_PROP_WINDOW_CREATE_VULKAN_BOOLEAN, true);
    SDL_SetBooleanProperty(props, SDL_PROP_WINDOW_CREATE_RESIZABLE_BOOLEAN, true);
    SDL_SetBooleanProperty(props, SDL_PROP_WINDOW_CREATE_HIGH_PIXEL_DENSITY_BOOLEAN, true);
    SDL_SetBooleanProperty(props, SDL_PROP_WINDOW_CREATE_HIDDEN_BOOLEAN, !config_.visible);
    SDL_SetNumberProperty(props, SDL_PROP_WINDOW_CREATE_WIDTH_NUMBER, config_.width);
    SDL_SetNumberProperty(props, SDL_PROP_WINDOW_CREATE_HEIGHT_NUMBER, config_.height);
    SDL_SetStringProperty(props, SDL_PROP_WINDOW_CREATE_TITLE_STRING, config_.title);
    window_ = SDL_CreateWindowWithProperties(props);
    SDL_DestroyProperties(props);
    sdl_check(window_ != nullptr, "SDL_CreateWindowWithProperties");
}

void Renderer::init_instance() {
    vkb::InstanceBuilder builder;
    builder.set_app_name(config_.title)
        .require_api_version(1, 3, 0)
        .set_debug_callback(debug_callback);
    if (config_.validation) {
        builder.request_validation_layers(true);
    }

    auto ret = builder.build();
    if (!ret) {
        // vkb reports two kinds of failure: a VkResult from the driver, and its
        // own error code when nothing suitable was found. Surface whichever
        // applies -- the test distinguishes them to decide skip vs fail.
        if (ret.vk_result() != VK_SUCCESS) {
            vk_check(ret.vk_result(), "vkb::InstanceBuilder::build");
        }
        throw vulkan_error(VK_ERROR_INITIALIZATION_FAILED, "vkb::InstanceBuilder::build: " + ret.error().message());
    }
    instance_ = std::move(ret).value();

    VkSurfaceKHR surface = VK_NULL_HANDLE;
    sdl_check(
        SDL_Vulkan_CreateSurface(window_, instance_.instance, instance_.allocation_callbacks, &surface),
        "SDL_Vulkan_CreateSurface"
    );
    surface_ = surface;
}

void Renderer::init_device() {
    // Vulkan 1.3 core features the renderer relies on: synchronization2 for the
    // barrier/submit forms used throughout, dynamicRendering so no render pass
    // objects are needed.
    VkPhysicalDeviceVulkan13Features features13{};
    features13.sType = VK_STRUCTURE_TYPE_PHYSICAL_DEVICE_VULKAN_1_3_FEATURES;
    features13.synchronization2 = VK_TRUE;
    features13.dynamicRendering = VK_TRUE;

    VkPhysicalDeviceVulkan12Features features12{};
    features12.sType = VK_STRUCTURE_TYPE_PHYSICAL_DEVICE_VULKAN_1_2_FEATURES;
    features12.descriptorIndexing = VK_TRUE;
    features12.timelineSemaphore = VK_TRUE;
    features12.bufferDeviceAddress = VK_TRUE;

    auto ret = vkb::PhysicalDeviceSelector(instance_)
                   .set_minimum_version(1, 3)
                   .set_required_features_13(features13)
                   .set_required_features_12(features12)
                   // Push descriptors: the gradient shader's one storage image
                   // is pushed inline rather than allocated from a pool.
                   .add_required_extension(VK_KHR_PUSH_DESCRIPTOR_EXTENSION_NAME)
                   .set_surface(surface_)
                   .select();
    if (!ret) {
        if (ret.vk_result() != VK_SUCCESS) {
            vk_check(ret.vk_result(), "vkb::PhysicalDeviceSelector::select");
        }
        // No device met the requirements. VK_ERROR_FEATURE_NOT_PRESENT is the
        // honest result code, and is what the test treats as "skip".
        throw vulkan_error(
            VK_ERROR_FEATURE_NOT_PRESENT, "vkb::PhysicalDeviceSelector::select: " + ret.error().message()
        );
    }
    physical_device_ = std::move(ret).value();

    auto dev_ret = vkb::DeviceBuilder(physical_device_).build();
    if (!dev_ret) {
        if (dev_ret.vk_result() != VK_SUCCESS) {
            vk_check(dev_ret.vk_result(), "vkb::DeviceBuilder::build");
        }
        throw vulkan_error(VK_ERROR_INITIALIZATION_FAILED, "vkb::DeviceBuilder::build: " + dev_ret.error().message());
    }
    device_ = std::move(dev_ret).value();

    auto queue = device_.get_queue(vkb::QueueType::graphics);
    if (!queue) {
        throw vulkan_error(VK_ERROR_INITIALIZATION_FAILED, "no graphics queue: " + queue.error().message());
    }
    graphics_queue_ = queue.value();
    graphics_queue_family_ = device_.get_queue_index(vkb::QueueType::graphics).value();

    // VK_KHR_push_descriptor was required at device selection, so a null here
    // means the loader disagrees with the selector -- fail loudly rather than
    // crash on the first dispatch.
    cmd_push_descriptor_set_ =
        reinterpret_cast<PFN_vkCmdPushDescriptorSetKHR>(vkGetDeviceProcAddr(device_.device, "vkCmdPushDescriptorSetKHR")
        );
    if (cmd_push_descriptor_set_ == nullptr) {
        throw vulkan_error(VK_ERROR_EXTENSION_NOT_PRESENT, "vkCmdPushDescriptorSetKHR not available");
    }
}

void Renderer::init_allocator() {
    VmaAllocatorCreateInfo info{};
    info.physicalDevice = physical_device_.physical_device;
    info.device = device_.device;
    info.instance = instance_.instance;
    info.flags = VMA_ALLOCATOR_CREATE_BUFFER_DEVICE_ADDRESS_BIT;
    vk_check(vmaCreateAllocator(&info, &allocator_), "vmaCreateAllocator");
}

void Renderer::init_commands() {
    frames_.resize(FRAME_OVERLAP);
    for (auto& frame : frames_) {
        VkCommandPoolCreateInfo pool_info{};
        pool_info.sType = VK_STRUCTURE_TYPE_COMMAND_POOL_CREATE_INFO;
        pool_info.flags = VK_COMMAND_POOL_CREATE_RESET_COMMAND_BUFFER_BIT;
        pool_info.queueFamilyIndex = graphics_queue_family_;
        vk_check(vkCreateCommandPool(device_.device, &pool_info, nullptr, &frame.command_pool), "vkCreateCommandPool");

        VkCommandBufferAllocateInfo alloc_info{};
        alloc_info.sType = VK_STRUCTURE_TYPE_COMMAND_BUFFER_ALLOCATE_INFO;
        alloc_info.commandPool = frame.command_pool;
        alloc_info.level = VK_COMMAND_BUFFER_LEVEL_PRIMARY;
        alloc_info.commandBufferCount = 1;
        vk_check(
            vkAllocateCommandBuffers(device_.device, &alloc_info, &frame.command_buffer), "vkAllocateCommandBuffers"
        );

        // Created signalled: frame 0 waits on this fence before it has ever
        // submitted anything, and an unsignalled fence would deadlock there.
        VkFenceCreateInfo fence_info{};
        fence_info.sType = VK_STRUCTURE_TYPE_FENCE_CREATE_INFO;
        fence_info.flags = VK_FENCE_CREATE_SIGNALED_BIT;
        vk_check(vkCreateFence(device_.device, &fence_info, nullptr, &frame.render_fence), "vkCreateFence");

        VkSemaphoreCreateInfo sem_info{};
        sem_info.sType = VK_STRUCTURE_TYPE_SEMAPHORE_CREATE_INFO;
        vk_check(
            vkCreateSemaphore(device_.device, &sem_info, nullptr, &frame.swapchain_semaphore), "vkCreateSemaphore"
        );
    }
}

void Renderer::init_descriptors() {
    VkDescriptorSetLayoutBinding binding{};
    binding.binding = 0;
    binding.descriptorType = VK_DESCRIPTOR_TYPE_STORAGE_IMAGE;
    binding.descriptorCount = 1;
    binding.stageFlags = VK_SHADER_STAGE_COMPUTE_BIT;

    VkDescriptorSetLayoutCreateInfo info{};
    info.sType = VK_STRUCTURE_TYPE_DESCRIPTOR_SET_LAYOUT_CREATE_INFO;
    // Push descriptors, so there is no pool and no set to allocate or free.
    info.flags = VK_DESCRIPTOR_SET_LAYOUT_CREATE_PUSH_DESCRIPTOR_BIT_KHR;
    info.bindingCount = 1;
    info.pBindings = &binding;
    vk_check(
        vkCreateDescriptorSetLayout(device_.device, &info, nullptr, &draw_image_descriptor_layout_),
        "vkCreateDescriptorSetLayout"
    );
}

void Renderer::init_pipeline() {
    VkPipelineLayoutCreateInfo layout_info{};
    layout_info.sType = VK_STRUCTURE_TYPE_PIPELINE_LAYOUT_CREATE_INFO;
    layout_info.setLayoutCount = 1;
    layout_info.pSetLayouts = &draw_image_descriptor_layout_;
    vk_check(
        vkCreatePipelineLayout(device_.device, &layout_info, nullptr, &gradient_pipeline_layout_),
        "vkCreatePipelineLayout"
    );

    // Compiled by CMake next to the test binary; see the module's CMakeLists.
    std::filesystem::path shader_path = executable_directory() / "shaders/gradient.slang.spv";
    if (const char* buck_shader_path = std::getenv("VULKAN_SHADER_PATH");
        buck_shader_path != nullptr && *buck_shader_path != '\0') {
        shader_path = buck_shader_path;
    }
    VkShaderModule shader = load_shader_module(shader_path, device_.device);

    VkPipelineShaderStageCreateInfo stage{};
    stage.sType = VK_STRUCTURE_TYPE_PIPELINE_SHADER_STAGE_CREATE_INFO;
    stage.stage = VK_SHADER_STAGE_COMPUTE_BIT;
    stage.module = shader;
    // slangc rewrites the entry point's name to "main" in the SPIR-V it emits,
    // whatever the function is called in the .slang source.
    stage.pName = "main";

    VkComputePipelineCreateInfo pipeline_info{};
    pipeline_info.sType = VK_STRUCTURE_TYPE_COMPUTE_PIPELINE_CREATE_INFO;
    pipeline_info.layout = gradient_pipeline_layout_;
    pipeline_info.stage = stage;

    VkResult r =
        vkCreateComputePipelines(device_.device, VK_NULL_HANDLE, 1, &pipeline_info, nullptr, &gradient_pipeline_);
    vkDestroyShaderModule(device_.device, shader, nullptr);
    vk_check(r, "vkCreateComputePipelines");
}

Renderer::~Renderer() {
    teardown();
}

void Renderer::teardown() noexcept {
    if (device_.device != VK_NULL_HANDLE) {
        // Nothing may be destroyed while the GPU might still be reading it.
        vkDeviceWaitIdle(device_.device);
    }

    // This one collects any still-pending screenshot before freeing the
    // memory, so it can throw (a failed readback). During teardown that is not
    // worth propagating -- and must not, out of a destructor.
    try {
        destroy_screenshot_buffers();
    } catch (const std::exception& e) {
        std::fprintf(stderr, "[vulkan] error while releasing screenshot buffers: %s\n", e.what());
        for (auto& img : screenshot_buffers_) {
            destroy_image(img);
        }
        screenshot_buffers_.clear();
    }
    destroy_draw_image();
    destroy_swapchain_image_data();
    destroy_swapchain();

    if (device_.device != VK_NULL_HANDLE) {
        if (gradient_pipeline_ != VK_NULL_HANDLE) {
            vkDestroyPipeline(device_.device, gradient_pipeline_, nullptr);
            gradient_pipeline_ = VK_NULL_HANDLE;
        }
        if (gradient_pipeline_layout_ != VK_NULL_HANDLE) {
            vkDestroyPipelineLayout(device_.device, gradient_pipeline_layout_, nullptr);
            gradient_pipeline_layout_ = VK_NULL_HANDLE;
        }
        if (draw_image_descriptor_layout_ != VK_NULL_HANDLE) {
            vkDestroyDescriptorSetLayout(device_.device, draw_image_descriptor_layout_, nullptr);
            draw_image_descriptor_layout_ = VK_NULL_HANDLE;
        }
        for (auto& frame : frames_) {
            if (frame.swapchain_semaphore != VK_NULL_HANDLE) {
                vkDestroySemaphore(device_.device, frame.swapchain_semaphore, nullptr);
            }
            if (frame.render_fence != VK_NULL_HANDLE) {
                vkDestroyFence(device_.device, frame.render_fence, nullptr);
            }
            // The buffer is freed with its pool.
            if (frame.command_pool != VK_NULL_HANDLE) {
                vkDestroyCommandPool(device_.device, frame.command_pool, nullptr);
            }
        }
        frames_.clear();
    }

    if (allocator_ != nullptr) {
        vmaDestroyAllocator(allocator_);
        allocator_ = nullptr;
    }
    if (device_.device != VK_NULL_HANDLE) {
        vkb::destroy_device(device_);
        device_ = {};
    }
    if (surface_ != VK_NULL_HANDLE) {
        vkb::destroy_surface(instance_, surface_);
        surface_ = VK_NULL_HANDLE;
    }
    if (instance_.instance != VK_NULL_HANDLE) {
        vkb::destroy_instance(instance_);
        instance_ = {};
    }
    if (window_ != nullptr) {
        SDL_DestroyWindow(window_);
        window_ = nullptr;
    }
    if (sdl_initialized_) {
        SDL_Quit();
        sdl_initialized_ = false;
    }
}

AllocatedImage Renderer::allocate_image(
    VkFormat format, VkImageUsageFlags usage, VkExtent3D extent, const VmaAllocationCreateInfo& alloc_info,
    VkImageTiling tiling, bool with_view
) {
    VkImageCreateInfo info = image_create_info(format, usage, extent);
    info.tiling = tiling;

    AllocatedImage img{};
    vk_check(
        vmaCreateImage(allocator_, &info, &alloc_info, &img.image, &img.allocation, &img.allocation_info),
        "vmaCreateImage"
    );
    img.extent = extent;
    img.format = format;

    if (with_view) {
        VkImageViewCreateInfo view_info = imageview_create_info(format, img.image, VK_IMAGE_ASPECT_COLOR_BIT);
        VkResult r = vkCreateImageView(device_.device, &view_info, nullptr, &img.view);
        if (r != VK_SUCCESS) {
            vmaDestroyImage(allocator_, img.image, img.allocation);
            vk_check(r, "vkCreateImageView");
        }
    }
    return img;
}

void Renderer::destroy_image(AllocatedImage& img) {
    if (img.view != VK_NULL_HANDLE) {
        vkDestroyImageView(device_.device, img.view, nullptr);
        img.view = VK_NULL_HANDLE;
    }
    if (img.image != VK_NULL_HANDLE) {
        vmaDestroyImage(allocator_, img.image, img.allocation);
        img.image = VK_NULL_HANDLE;
        img.allocation = nullptr;
    }
}

void Renderer::create_swapchain() {
    int w = 0, h = 0;
    sdl_check(SDL_GetWindowSizeInPixels(window_, &w, &h), "SDL_GetWindowSizeInPixels");

    destroy_swapchain();

    VkSurfaceFormatKHR desired{};
    desired.format = VK_FORMAT_B8G8R8A8_UNORM;
    desired.colorSpace = VK_COLOR_SPACE_SRGB_NONLINEAR_KHR;

    auto ret = vkb::SwapchainBuilder{physical_device_.physical_device, device_.device, surface_}
                   .set_desired_format(desired)
                   .set_desired_present_mode(config_.present_mode)
                   .set_desired_extent(static_cast<uint32_t>(w), static_cast<uint32_t>(h))
                   // The frame is rendered into an offscreen image and blitted
                   // here, so the swapchain image is a transfer destination.
                   .add_image_usage_flags(VK_IMAGE_USAGE_TRANSFER_DST_BIT)
                   .build();
    if (!ret) {
        if (ret.vk_result() != VK_SUCCESS) {
            vk_check(ret.vk_result(), "vkb::SwapchainBuilder::build");
        }
        throw vulkan_error(VK_ERROR_INITIALIZATION_FAILED, "vkb::SwapchainBuilder::build: " + ret.error().message());
    }
    swapchain_ = std::move(ret).value();
    has_swapchain_ = true;

    auto images = swapchain_.get_images();
    if (!images) {
        throw vulkan_error(VK_ERROR_INITIALIZATION_FAILED, "swapchain get_images: " + images.error().message());
    }
    swapchain_images_ = std::move(images).value();
}

void Renderer::destroy_swapchain() {
    if (has_swapchain_) {
        // The builder created the views; it also owns their destruction.
        auto views = swapchain_.get_image_views();
        if (views) {
            swapchain_.destroy_image_views(views.value());
        }
        vkb::destroy_swapchain(swapchain_);
        swapchain_ = {};
        has_swapchain_ = false;
    }
    swapchain_images_.clear();
}

void Renderer::create_swapchain_image_data() {
    destroy_swapchain_image_data();
    render_semaphores_.reserve(swapchain_.image_count);
    for (uint32_t i = 0; i < swapchain_.image_count; ++i) {
        VkSemaphoreCreateInfo info{};
        info.sType = VK_STRUCTURE_TYPE_SEMAPHORE_CREATE_INFO;
        VkSemaphore sem = VK_NULL_HANDLE;
        vk_check(vkCreateSemaphore(device_.device, &info, nullptr, &sem), "vkCreateSemaphore");
        render_semaphores_.push_back(sem);
    }
}

void Renderer::destroy_swapchain_image_data() {
    if (device_.device != VK_NULL_HANDLE) {
        for (VkSemaphore sem : render_semaphores_) {
            vkDestroySemaphore(device_.device, sem, nullptr);
        }
    }
    render_semaphores_.clear();
}

void Renderer::create_draw_image() {
    destroy_draw_image();

    VkExtent3D extent{swapchain_.extent.width, swapchain_.extent.height, 1};
    VkImageUsageFlags usage = VK_IMAGE_USAGE_TRANSFER_SRC_BIT | VK_IMAGE_USAGE_TRANSFER_DST_BIT
        | VK_IMAGE_USAGE_STORAGE_BIT | VK_IMAGE_USAGE_COLOR_ATTACHMENT_BIT;

    VmaAllocationCreateInfo alloc{};
    alloc.usage = VMA_MEMORY_USAGE_GPU_ONLY;
    alloc.requiredFlags = VK_MEMORY_PROPERTY_DEVICE_LOCAL_BIT;

    draw_image_ = allocate_image(VK_FORMAT_R16G16B16A16_SFLOAT, usage, extent, alloc);
}

void Renderer::destroy_draw_image() {
    if (draw_image_) {
        destroy_image(*draw_image_);
        draw_image_.reset();
    }
}

void Renderer::create_screenshot_buffers() {
    destroy_screenshot_buffers();

    // Downscale to at most screenshot_max_dim on the long edge, preserving
    // aspect ratio. Both edges are kept a multiple of 4 so the result is
    // friendly to block-compressed consumers.
    const uint32_t max_dim = config_.screenshot_max_dim;
    uint32_t buf_w = 0, buf_h = 0;
    {
        int screen_w = 0, screen_h = 0;
        sdl_check(SDL_GetWindowSizeInPixels(window_, &screen_w, &screen_h), "SDL_GetWindowSizeInPixels");
        if (screen_w >= screen_h) {
            buf_w = max_dim;
            buf_h = round_up(static_cast<uint32_t>(float(screen_h) / float(screen_w) * float(buf_w)), 4);
        } else {
            buf_h = max_dim;
            buf_w = round_up(static_cast<uint32_t>(float(screen_w) / float(screen_h) * float(buf_h)), 4);
        }
        buf_w = std::max(buf_w, 4u);
        buf_h = std::max(buf_h, 4u);
    }

    for (unsigned i = 0; i < FRAME_OVERLAP; ++i) {
        VkImageUsageFlags usage =
            VK_IMAGE_USAGE_TRANSFER_SRC_BIT | VK_IMAGE_USAGE_TRANSFER_DST_BIT | VK_IMAGE_USAGE_STORAGE_BIT;

        // Host-visible and permanently mapped, so reading a completed frame is
        // a memcpy with no staging buffer and no extra submit. Linear tiling is
        // what makes vkGetImageSubresourceLayout meaningful for the readback.
        VmaAllocationCreateInfo alloc{};
        alloc.usage = VMA_MEMORY_USAGE_AUTO;
        alloc.requiredFlags = VK_MEMORY_PROPERTY_HOST_VISIBLE_BIT | VK_MEMORY_PROPERTY_HOST_COHERENT_BIT;
        alloc.preferredFlags = VK_MEMORY_PROPERTY_HOST_CACHED_BIT;
        alloc.flags = VMA_ALLOCATION_CREATE_MAPPED_BIT | VMA_ALLOCATION_CREATE_HOST_ACCESS_RANDOM_BIT;

        screenshot_buffers_.push_back(
            allocate_image(
                VK_FORMAT_R8G8B8A8_UNORM, usage, VkExtent3D{buf_w, buf_h, 1}, alloc, VK_IMAGE_TILING_LINEAR,
                /*with_view=*/false
            )
        );
    }
}

void Renderer::destroy_screenshot_buffers() {
    // A buffer with a blit still pending has already had its fence waited on by
    // whoever is tearing down (recreate waits idle, the destructor waits idle),
    // so its contents are readable right now -- collect them before the memory
    // goes away rather than losing the frame.
    if (screenshot_buffers_.size() == FRAME_OVERLAP) {
        for (uint64_t frame = frame_number_ - std::min<uint64_t>(frame_number_, FRAME_OVERLAP); frame < frame_number_;
             ++frame) {
            auto& f = frames_[frame % FRAME_OVERLAP];
            if (f.pending_screenshot && f.screenshot_frame == frame) {
                latest_screenshot_ = read_screenshot(frame);
                f.pending_screenshot = false;
            }
        }
    }
    for (auto& img : screenshot_buffers_) {
        destroy_image(img);
    }
    screenshot_buffers_.clear();
}

void Renderer::recreate_draw_structures() {
    vkDeviceWaitIdle(device_.device);
    create_swapchain();
    create_swapchain_image_data();
    create_draw_image();
    create_screenshot_buffers();
}

bool Renderer::poll_events() {
    SDL_Event e;
    while (SDL_PollEvent(&e)) {
        if (e.type == SDL_EVENT_QUIT) {
            should_quit_ = true;
        }
        if (e.type == SDL_EVENT_WINDOW_CLOSE_REQUESTED && window_ != nullptr
            && e.window.windowID == SDL_GetWindowID(window_)) {
            should_quit_ = true;
        }
        if (e.type == SDL_EVENT_WINDOW_PIXEL_SIZE_CHANGED) {
            needs_new_swapchain_ = true;
        }
    }
    return !should_quit_;
}

Screenshot Renderer::read_screenshot(uint64_t frame) {
    auto& img = screenshot_buffers_[frame % FRAME_OVERLAP];

    // The GPU picked the row stride; undo it so the result is tightly packed.
    VkImageSubresource subresource{};
    subresource.aspectMask = VK_IMAGE_ASPECT_COLOR_BIT;
    subresource.mipLevel = 0;
    subresource.arrayLayer = 0;
    VkSubresourceLayout layout{};
    vkGetImageSubresourceLayout(device_.device, img.image, &subresource, &layout);

    Screenshot shot;
    shot.width = img.extent.width;
    shot.height = img.extent.height;
    shot.pixels.resize(static_cast<size_t>(shot.width) * shot.height * 4);

    const auto* src_base = static_cast<const uint8_t*>(img.allocation_info.pMappedData);
    if (src_base == nullptr) {
        throw vulkan_error(VK_ERROR_MEMORY_MAP_FAILED, "screenshot buffer is not mapped");
    }
    for (uint32_t y = 0; y < shot.height; ++y) {
        std::memcpy(
            shot.pixels.data() + static_cast<size_t>(y) * shot.width * 4,
            src_base + layout.offset + layout.rowPitch * y, static_cast<size_t>(shot.width) * 4
        );
    }
    return shot;
}

std::optional<Screenshot> Renderer::take_screenshot() {
    return latest_screenshot_;
}

std::optional<Screenshot> Renderer::take_screenshot_blocking() {
    // Every submitted frame is complete once the device is idle, so any pending
    // screenshot buffer can be read without further synchronization.
    if (device_.device != VK_NULL_HANDLE) {
        vk_check(vkDeviceWaitIdle(device_.device), "vkDeviceWaitIdle");
    }
    if (screenshot_buffers_.size() == FRAME_OVERLAP) {
        // Walk the in-flight window oldest-first so the newest pending frame
        // wins.
        for (uint64_t frame = frame_number_ - std::min<uint64_t>(frame_number_, FRAME_OVERLAP); frame < frame_number_;
             ++frame) {
            auto& f = frames_[frame % FRAME_OVERLAP];
            if (f.pending_screenshot && f.screenshot_frame == frame) {
                latest_screenshot_ = read_screenshot(frame);
                f.pending_screenshot = false;
            }
        }
    }
    return latest_screenshot_;
}

} // namespace vulkan_module
