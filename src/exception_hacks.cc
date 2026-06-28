#include "doctest/doctest.h"
#include <dlfcn.h>
#include <stacktrace>
#include <cstring>
#include <exception>
#include <stdexcept>
#include <cstdlib>
#include <memory>
#include <algorithm>
#include <ranges>
#include <string>
#include <string_view>

constexpr auto exception_offset = 256;

extern "C"
void *__cxa_allocate_exception(size_t thrown_size) {
    using type = void*(*)(size_t);
    auto original_func = reinterpret_cast<type>(dlsym(RTLD_NEXT, "__cxa_allocate_exception"));
    auto mem = original_func(thrown_size + exception_offset);
    memset(reinterpret_cast<char*>(mem), 0, exception_offset);
    new (mem) std::stacktrace(std::stacktrace::current(1));
    return reinterpret_cast<char*>(mem) + exception_offset;
}

extern "C"
void __cxa_free_exception(void *mem) {
    using type = void(*)(void*);
    auto original_func = reinterpret_cast<type>(dlsym(RTLD_NEXT, "__cxa_free_exception"));
    auto orig_mem = reinterpret_cast<char*>(mem) - exception_offset;
    std::destroy_at<std::stacktrace>(reinterpret_cast<std::stacktrace*>(orig_mem));
    original_func(orig_mem);
}

const std::stacktrace& stacktrace_of_exception(const std::exception_ptr& eptr) {
    void* mem;
    std::memcpy(&mem, &eptr, 8);
    const std::stacktrace* trace = reinterpret_cast<std::stacktrace*>(static_cast<char*>(mem) - exception_offset);
    return *trace;
}

TEST_SUITE("Exception stacktrace tests") {
    TEST_CASE("Exception stacktrace sanity") {
        try {
            throw std::runtime_error("Error");
        } catch (...) {
            auto st = stacktrace_of_exception(std::current_exception());
            auto function_name = std::string_view(__FUNCTION__);
            CHECK(st.size() > 1);
            CHECK(std::ranges::any_of(st, [&](const std::stacktrace_entry& frame) {
                return frame.description().contains(function_name);
            }));
            CHECK(std::ranges::any_of(st, [](const std::stacktrace_entry& frame) {
                return frame.description().contains("main");
            }));
        }
    }
}
