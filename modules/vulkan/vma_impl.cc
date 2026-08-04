// The one translation unit that instantiates VulkanMemoryAllocator. VMA is
// header-only and guards its implementation behind this macro, so exactly one
// TU in the module may define it.
#define VMA_IMPLEMENTATION
#include <vk_mem_alloc.h>
