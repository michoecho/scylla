#pragma once

// Keep this list limited to headers shared by the Buck C++ modules. The
// cxx_precompiled_header target supplies the corresponding external include
// paths and libraries when PCH is enabled.
#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <filesystem>
#include <format>
#include <fstream>
#include <functional>
#include <iomanip>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <source_location>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <cerrno>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <boost/stacktrace/stacktrace.hpp>
#include <boost/stacktrace/this_thread.hpp>
#include <doctest/doctest.h>
#include <hegel/hegel.h>
#include <hegel/settings.h>
#include <SDL3/SDL.h>
#include <SDL3/SDL_vulkan.h>
#include <VkBootstrap.h>
#include <vk_mem_alloc.h>
#include <vulkan/vulkan.h>
