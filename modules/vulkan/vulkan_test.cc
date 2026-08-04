// The module's success criterion: the screenshot mechanism returns a scaled
// down image of what was drawn on screen.
//
// "Scaled down" and "what was drawn" are both checked directly against pixels.
// The gradient shader draws a known image -- red rising left-to-right, green
// rising top-to-bottom, and a blue filled circle of radius min(w,h)/4 at the
// centre -- so the screenshot can be asserted on without a golden file: the
// gradient pins orientation and scale, the circle pins that this is drawn
// content and not a cleared or stale buffer.
//
// The test needs a real GPU and a display. Where either is missing it skips
// rather than fails, so the suite still passes on a headless machine; a real
// Vulkan error anywhere else is still a failure.

#include <cstdlib>
#include <optional>
#include <string>

#include <doctest/doctest.h>

#include "vulkan_module/renderer.h"

using vulkan_module::Renderer;
using vulkan_module::Screenshot;

namespace {

// Window size the tests render at. Deliberately non-square and larger than the
// screenshot's long edge, so a screenshot that came back at window resolution
// (i.e. not scaled) fails the size assertions below.
constexpr int kWindowWidth = 640;
constexpr int kWindowHeight = 400;
constexpr uint32_t kScreenshotMaxDim = 120;

// True when there is no display server for SDL to open a window on. SDL will
// fail at init in that case, and that is an environment fact, not a bug.
bool no_display() {
    const char* wayland = std::getenv("WAYLAND_DISPLAY");
    const char* x11 = std::getenv("DISPLAY");
    return (wayland == nullptr || *wayland == '\0') && (x11 == nullptr || *x11 == '\0');
}

Renderer::Config test_config() {
    Renderer::Config config;
    config.title = "vulkan module test";
    config.width = kWindowWidth;
    config.height = kWindowHeight;
    // Nothing here looks at the window, and a test suite should not steal
    // focus. The swapchain still presents.
    config.visible = false;
    config.screenshot_max_dim = kScreenshotMaxDim;
    // Do not pace the test to the display. Under FIFO an acquire blocks until
    // the compositor hands an image back, which for a window nobody is
    // compositing (hidden, or the screen locked) means waiting out the 2s
    // acquire timeout every frame. MAILBOX recycles images without waiting for
    // a vblank, so the frames here complete as fast as the GPU draws them.
    config.present_mode = VK_PRESENT_MODE_MAILBOX_KHR;
    return config;
}

// Builds a Renderer, or returns nullptr when this machine cannot run one.
// Anything other than "no suitable device" propagates.
std::unique_ptr<Renderer> try_make_renderer() {
    if (no_display()) {
        return nullptr;
    }
    try {
        return std::make_unique<Renderer>(test_config());
    } catch (const vulkan_module::sdl_error& e) {
        MESSAGE("skipping: SDL could not start: " << e.what());
        return nullptr;
    } catch (const vulkan_module::vulkan_error& e) {
        if (e.result == VK_ERROR_FEATURE_NOT_PRESENT || e.result == VK_ERROR_INCOMPATIBLE_DRIVER
            || e.result == VK_ERROR_INITIALIZATION_FAILED) {
            MESSAGE("skipping: no usable Vulkan device: " << e.what());
            return nullptr;
        }
        throw;
    }
}

// Returns std::string, not const char*: doctest's MESSAGE streams a character
// pointer as a pointer.
std::string present_mode_name(VkPresentModeKHR mode) {
    switch (mode) {
    case VK_PRESENT_MODE_IMMEDIATE_KHR:
        return "IMMEDIATE";
    case VK_PRESENT_MODE_MAILBOX_KHR:
        return "MAILBOX";
    case VK_PRESENT_MODE_FIFO_KHR:
        return "FIFO";
    case VK_PRESENT_MODE_FIFO_RELAXED_KHR:
        return "FIFO_RELAXED";
    default:
        return "<other>";
    }
}

// Mean channel values over a box, so a single filtered pixel cannot decide an
// assertion.
struct MeanRgb {
    double r = 0, g = 0, b = 0;
};

MeanRgb mean_over(const Screenshot& shot, uint32_t x0, uint32_t y0, uint32_t x1, uint32_t y1) {
    MeanRgb sum;
    uint32_t n = 0;
    for (uint32_t y = y0; y < y1; ++y) {
        for (uint32_t x = x0; x < x1; ++x) {
            auto px = shot.at(x, y);
            sum.r += px.r;
            sum.g += px.g;
            sum.b += px.b;
            ++n;
        }
    }
    REQUIRE(n > 0);
    return MeanRgb{sum.r / n, sum.g / n, sum.b / n};
}

} // namespace

TEST_CASE("Renderer screenshot returns a scaled down image of what was drawn") {
    auto renderer = try_make_renderer();
    if (!renderer) {
        return; // Skipped; see no_display()/try_make_renderer above.
    }

    MESSAGE("device: " << renderer->device_name());
    // Not asserted: a surface that only supports FIFO is still perfectly able
    // to pass this test, just more slowly.
    MESSAGE("present mode: " << present_mode_name(renderer->present_mode()));

    // Before anything has been drawn there is nothing to hand back.
    CHECK_FALSE(renderer->take_screenshot().has_value());

    // A handful of frames: enough that at least one completes and gets read
    // back through the ordinary in-flight path, and that an initial
    // out-of-date swapchain (common on the first present after a window is
    // mapped) does not leave the test with zero frames.
    for (int i = 0; i < 8; ++i) {
        renderer->poll_events();
        renderer->draw_frame();
    }
    REQUIRE(renderer->frames_presented() > 0);

    auto maybe_shot = renderer->take_screenshot_blocking();
    REQUIRE(maybe_shot.has_value());
    const Screenshot& shot = *maybe_shot;

    SUBCASE("it is scaled down from the rendered surface") {
        const VkExtent2D drawn = renderer->swapchain_extent();

        // Strictly smaller than what was rendered, in both axes.
        CHECK(shot.width < drawn.width);
        CHECK(shot.height < drawn.height);

        // The long edge is exactly the configured cap.
        CHECK(std::max(shot.width, shot.height) == kScreenshotMaxDim);

        // Aspect ratio is preserved, up to the rounding of the short edge up to
        // a multiple of 4.
        const double drawn_aspect = double(drawn.width) / double(drawn.height);
        const double shot_aspect = double(shot.width) / double(shot.height);
        CHECK(shot_aspect == doctest::Approx(drawn_aspect).epsilon(0.05));

        // Both edges are multiples of 4, as the buffer sizing promises.
        CHECK(shot.width % 4 == 0);
        CHECK(shot.height % 4 == 0);

        // And the buffer really holds that many tightly packed RGBA pixels.
        CHECK(shot.pixels.size() == size_t(shot.width) * shot.height * 4);
    }

    SUBCASE("it holds the drawn image, not a blank buffer") {
        // The shader writes alpha 1 everywhere it runs.
        auto centre = shot.at(shot.width / 2, shot.height / 2);
        CHECK(centre.a == 255);

        // Not a uniform buffer: a cleared or never-written image would have no
        // variation at all.
        bool varies = false;
        auto first = shot.at(0, 0);
        for (uint32_t y = 0; y < shot.height && !varies; ++y) {
            for (uint32_t x = 0; x < shot.width; ++x) {
                auto px = shot.at(x, y);
                if (px.r != first.r || px.g != first.g || px.b != first.b) {
                    varies = true;
                    break;
                }
            }
        }
        CHECK(varies);
    }

    SUBCASE("the gradient runs the way it was drawn") {
        // Sample 1/8-sized boxes just inside each edge's midpoint, away from
        // the centre circle.
        const uint32_t bw = std::max(shot.width / 8, 2u);
        const uint32_t bh = std::max(shot.height / 8, 2u);
        const uint32_t mid_x = shot.width / 2 - bw / 2;
        const uint32_t mid_y = shot.height / 2 - bh / 2;

        const MeanRgb left = mean_over(shot, 0, mid_y, bw, mid_y + bh);
        const MeanRgb right = mean_over(shot, shot.width - bw, mid_y, shot.width, mid_y + bh);
        const MeanRgb top = mean_over(shot, mid_x, 0, mid_x + bw, bh);
        const MeanRgb bottom = mean_over(shot, mid_x, shot.height - bh, mid_x + bw, shot.height);

        // Red rises left to right. A vertically flipped or transposed
        // screenshot fails this pair.
        CHECK(right.r > left.r + 64);
        // Green rises top to bottom.
        CHECK(bottom.g > top.g + 64);

        // ...and each varies only along its own axis.
        CHECK(std::abs(top.r - bottom.r) < 32);
        CHECK(std::abs(left.g - right.g) < 32);
    }

    SUBCASE("the centre circle survives the downscale") {
        const uint32_t bw = std::max(shot.width / 10, 2u);
        const uint32_t bh = std::max(shot.height / 10, 2u);

        // Inside the circle (radius min(w,h)/4 about the centre) blue is 1;
        // outside it is 0.
        const MeanRgb inside =
            mean_over(shot, shot.width / 2 - bw, shot.height / 2 - bh, shot.width / 2 + bw, shot.height / 2 + bh);
        const MeanRgb corner = mean_over(shot, 0, 0, bw, bh);

        CHECK(inside.b > 200);
        CHECK(corner.b < 55);
    }
}

TEST_CASE("Renderer screenshots track successive frames") {
    auto renderer = try_make_renderer();
    if (!renderer) {
        return;
    }

    // Drawing more frames than there are frame slots exercises the readback
    // path where a slot's screenshot is collected on its next reuse, rather
    // than only by the drain in take_screenshot_blocking().
    for (int i = 0; i < 6; ++i) {
        renderer->poll_events();
        renderer->draw_frame();
    }
    REQUIRE(renderer->frames_presented() > 0);

    // The scene is static, so every frame's screenshot is the same picture --
    // what matters is that a screenshot is available mid-run, without a
    // device-wide wait.
    auto mid_run = renderer->take_screenshot();
    REQUIRE(mid_run.has_value());
    CHECK(mid_run->width > 0);
    CHECK(mid_run->height > 0);
    CHECK(mid_run->at(mid_run->width / 2, mid_run->height / 2).a == 255);

    auto drained = renderer->take_screenshot_blocking();
    REQUIRE(drained.has_value());
    CHECK(drained->width == mid_run->width);
    CHECK(drained->height == mid_run->height);
}
