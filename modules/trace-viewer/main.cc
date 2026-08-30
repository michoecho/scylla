#include <implot.h>
#include <imgui/imgui.h>
#include <imgui/backends/imgui_impl_sdl3.h>
#include <imgui/backends/imgui_impl_opengl3.h>
#include <SDL3/SDL.h>
#include <SDL3/SDL_opengl.h>
#include <algorithm>
#include <stdio.h>
#include <math.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <span>
#include <functional>
#include <chrono>
#include <thread>
#include <vector>
#include <cerrno>
#include <atomic>
#include <stdexcept>
#include <system_error>
#include <fmt/core.h>
#include <fmt/ranges.h>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <unordered_map>
#include "decoder.h"

const double MULTIPLIER = 0.2941171840072451;

inline int64_t rdtsc() {
    uint64_t rax, rdx;
    asm volatile ( "rdtsc" : "=a" (rax), "=d" (rdx) );
    return (int64_t)(( rdx << 32 ) + rax);
}

// The viewer's own flat record.
//
// Scylla emits *named* tracepoints now (decoder.h, generated from the very
// binary that produced the trace), not the four raw words this analysis was
// originally written against. The numbers below are what those tracepoints are
// flattened back into, because everything downstream -- the query grouping, the
// io/cpu/starve accounting, the log formatting -- keys off them:
//
//   0  run_task{prev, task}         the reactor started running a task
//   1  cql_request{prev, task}      a CQL frame opened a new request chain
//   4  io_begin{task, io}           a task submitted an I/O
//   5  io_end{task, io}             that I/O completed
//   0xb execution_stage{prev, task} an execution stage ran a queued work item
//
// (0x3 and 0xa, the reader-concurrency-semaphore events of the original
// experiment, are not emitted by this build; the formatters for them are left
// in place.)
struct entry {
    uint64_t event;
    uint64_t id;
    uint64_t arg;
    int64_t ts;

    // Where the task this record is about was created, as an index into
    // locations() below. Zero is "none" -- most events carry no location at all,
    // and a task nobody gave a resume point to does not either.
    uint32_t loc = 0;

    // Which request this record belongs to. For a *switch* that is the task
    // being switched to; for everything else, the task it happened under.
    uint64_t query() const {
        if (event == 0 || event == 1 || event == 0xa || event == 0xb) {
            return arg;
        } else {
            return id;
        }
    }
};

// The decoded source locations, interned.
//
// run_task carries one per record and the same call site turns up thousands of
// times -- every continuation the reactor runs off one `then()` -- so the entries
// hold an index into this and not a string. Index 0 is the empty location, which
// is what an unlocated event and a task with no resume point both get.
static std::vector<std::string> location_strings{""};

static uint32_t intern_location(const trace::source_location& loc) {
    if (!loc.resolved && loc.address == 0) {
        return 0;
    }
    // By address, because that is the identity of a location -- two records of
    // the same call site are the same word -- and it is one integer compare
    // instead of a string one.
    static std::unordered_map<uint64_t, uint32_t> seen;
    const auto [it, fresh] = seen.emplace(loc.address, uint32_t(location_strings.size()));
    if (fresh) {
        // Just the tail of the path and the function: the log line this ends up
        // on is already wide, and "reactor.cc:1234" is what identifies a call
        // site to someone reading it.
        std::string file = loc.file;
        if (const auto slash = file.rfind('/'); slash != std::string::npos) {
            file = file.substr(slash + 1);
        }
        location_strings.push_back(loc.resolved
                                       ? fmt::format("{}:{}", file, loc.line)
                                       : loc.to_string());
    }
    return it->second;
}

static const std::string& location_string(uint32_t index) {
    return location_strings[index];
}

static std::string entry_message(const entry& e) {
    switch (e.event) {
    case 0: return fmt::format("{:10s} {}", "SWITCH", location_string(e.loc));
    case 1: return "START";
    case 0xa: return "PERMIT";
    case 0xb: return "ES";
    case 0x3: {
        const char* rcs_status[] = {
            "admitted immediately",
            "queued because of non-empty ready",
            "queued because of used permits",
            "queued because of memory resources",
            "queued because of count resources",
        };
        return fmt::format("{:10s} {}", "RCS", rcs_status[e.arg]);
    }
    case 0x4: return fmt::format("{:10s} {:16x}", "IO_BEGIN", e.arg);
    case 0x5: return fmt::format("{:10s} {:16x}", "IO_END", e.arg);
    default: return fmt::format("UNKNOWN ({})", e.event);
    }
}

struct cached_log_line {
    size_t source_index;
    entry record;
    std::string text;
};

struct log_cache {
    uint64_t task_id = 0;
    int threshold = -1;
    size_t item_count = 0;
    size_t source_begin = 0;
    std::vector<cached_log_line> lines;
};

struct cached_plot_item {
    ImPlotPoint min;
    ImPlotPoint max;
    ImPlotPoint line_end;
    ImU32 color;
    bool draw_line;
};

struct full_log_cache {
    uint64_t task_id = 0;
    int threshold = -1;
    size_t task_count = 0;
    size_t source_begin = 0;
    int64_t start_ts = 0;
    int64_t end_ts = 0;
    std::vector<cached_log_line> lines;
    std::vector<cached_plot_item> plot_items;
};

static std::string log_line_text(const entry& e, int64_t start_ts, bool include_task_id) {
    auto dt_nano = std::chrono::duration<double, std::nano>(double(e.ts - start_ts) * MULTIPLIER);
    auto dt = std::chrono::duration<double, std::milli>(dt_nano);
    if (include_task_id) {
        return fmt::format("{:12.9f}: {:16x}: {}", dt.count(), e.query(), entry_message(e));
    }
    return fmt::format("{:12.9f}: {}", dt.count(), entry_message(e));
}

static void render_truncation_warning(size_t count, int threshold, bool spanned) {
    ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(1.f, 0.f, 0.f, 1.f));
    if (spanned) {
        ImGui::Text("number of tasks spanned %zu is greater than configured threshold %d, not rendering the rest", count, threshold);
    } else {
        ImGui::Text("number of tasks %zu is greater than configured threshold %d, not rendering the rest", count, threshold);
    }
    ImGui::PopStyleColor();
}

static void update_log_cache(log_cache& cache, uint64_t task_id, int threshold,
                             const std::vector<entry>& sorted) {
    if (cache.task_id == task_id && cache.threshold == threshold) {
        return;
    }

    cache = {};
    cache.task_id = task_id;
    cache.threshold = threshold;
    auto range = std::ranges::equal_range(sorted, task_id, std::ranges::less(),
                                          [] (const auto& e) { return e.query(); });
    cache.item_count = range.size();
    cache.source_begin = range.begin() - sorted.begin();

    const size_t cached_count = std::min(cache.item_count, static_cast<size_t>(threshold));
    cache.lines.reserve(cached_count);
    const int64_t start_ts = range.front().ts;
    for (size_t source_index = cache.source_begin;
         source_index < cache.source_begin + cached_count; ++source_index) {
        const auto& record = sorted[source_index];
        cache.lines.push_back({source_index, record, log_line_text(record, start_ts, false)});
    }
}

static void update_full_log_cache(full_log_cache& cache, uint64_t task_id, int threshold,
                                  const std::vector<entry>& sorted,
                                  std::span<const entry> span) {
    if (cache.task_id == task_id && cache.threshold == threshold) {
        return;
    }

    cache = {};
    cache.task_id = task_id;
    cache.threshold = threshold;
    auto sorted_range = std::ranges::equal_range(sorted, task_id, std::ranges::less(),
                                                 [] (const auto& e) { return e.query(); });
    auto span_range = std::ranges::equal_range(
        span, 1, std::ranges::less(), [&sorted_range] (const auto& e) {
            return (e.ts >= sorted_range.front().ts) + (e.ts > sorted_range.back().ts);
    });
    cache.task_count = span_range.size();
    cache.source_begin = span_range.begin() - span.begin();

    cache.start_ts = sorted_range.front().ts;
    cache.end_ts = sorted_range.back().ts;
    const size_t cached_count = std::min(cache.task_count, static_cast<size_t>(threshold));
    cache.lines.reserve(cached_count);
    cache.plot_items.reserve(cached_count);
    for (size_t source_index = cache.source_begin;
         source_index < cache.source_begin + cached_count; ++source_index) {
        const auto& record = span[source_index];
        cache.lines.push_back({source_index, record, log_line_text(record, cache.start_ts, true)});
    }

    uint64_t iostack = 0;
    int64_t iostart = 0;
    int64_t prev_ts = cache.start_ts;
    bool cpu = true;
    for (size_t source_index = cache.source_begin;
         source_index < cache.source_begin + cached_count; ++source_index) {
        const auto& record = span[source_index];
        const double x_min = double(prev_ts - cache.start_ts) * MULTIPLIER / 1e6;
        const double x_max = double(record.ts - cache.start_ts) * MULTIPLIER / 1e6;
        cache.plot_items.push_back({
            {x_min, 1.0},
            {x_max, 0.0},
            {x_min, 0.0},
            cpu ? IM_COL32(0, 128, 0, 255) : IM_COL32(0, 0, 128, 32),
            cpu,
        });

        if (record.query() == task_id) {
            if (record.event != 0x5) {
                cpu = true;
            }
            if (record.event == 0x4) {
                if (iostack == 0) {
                    iostart = record.ts;
                }
                ++iostack;
            } else if (record.event == 0x5) {
                --iostack;
                if (iostack == 0) {
                    cache.plot_items.push_back({
                        {double(iostart - cache.start_ts) * MULTIPLIER / 1e6, 1.0},
                        {x_max, 0.0},
                        {},
                        IM_COL32(255, 255, 255, 32),
                        false,
                    });
                }
            }
        } else {
            cpu = false;
        }
        prev_ts = record.ts;
    }
}

// Task ids are *not* namespaced by shard here, deliberately. A request
// coordinated on one shard reaches a tablet on another, and the continuations
// that run there inherit its id -- so one request's records are spread over two
// shards' files under a single id, and separating them by shard would cut every
// cross-shard request in half. Scylla mints the ids with the shard already in
// the top bits, which is what makes that safe; see fresh_task_id in
// seastar/src/core/scylla_tracer.cc.

// The callback the generated decode() hands each record to: one overload per
// tracepoint the viewer has a use for, and a template that swallows the rest.
struct sink {
    std::vector<entry>& out;

    void operator()(const trace::run_task& e, const trace::tracepoint_metadata& m) const {
        out.push_back({0, e.prev, e.task, int64_t(m.timestamp), intern_location(e.at)});
    }
    void operator()(const trace::cql_request& e, const trace::tracepoint_metadata& m) const {
        out.push_back({1, e.prev, e.task, int64_t(m.timestamp)});
    }
    void operator()(const trace::execution_stage& e, const trace::tracepoint_metadata& m) const {
        out.push_back({0xb, e.prev, e.task, int64_t(m.timestamp)});
    }
    void operator()(const trace::io_begin& e, const trace::tracepoint_metadata& m) const {
        out.push_back({0x4, e.task, e.io, int64_t(m.timestamp)});
    }
    void operator()(const trace::io_end& e, const trace::tracepoint_metadata& m) const {
        out.push_back({0x5, e.task, e.io, int64_t(m.timestamp)});
    }
    template <typename Event>
    void operator()(const Event&, const trace::tracepoint_metadata&) const {}
};

// One shard's trace file, decoded into the records above.
static void load_trace(const std::filesystem::path& path, std::vector<entry>& out,
                       trace::dso_directory& dsos) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        throw std::system_error(errno, std::generic_category(), path.string());
    }
    const std::vector<char> raw{std::istreambuf_iterator<char>(in),
                                std::istreambuf_iterator<char>()};
    trace::decode({reinterpret_cast<const std::byte*>(raw.data()), raw.size()},
                  sink{out}, dsos);
}
template <> struct fmt::formatter<entry> : formatter<string_view> {
    auto format(const entry& e, auto& ctx) const -> decltype(ctx.out()) {
        // ctx.out() is an output iterator to write to.
        return fmt::format_to(ctx.out(), "({:016x} {:016x} {:016x} {:016x})", e.event, e.id, e.arg, e.ts);
    }
};

int main(int argc, char** argv) {
    if (argc != 2) {
        fprintf(stderr, "usage: %s SNAPSHOT-DIR\n", argv[0]);
        fprintf(stderr, "  a directory of shard-N.trace files, as written by Scylla's\n"
                        "  POST /system/trace_snapshot into <workdir>/traces/<stamp>/\n");
        return 2;
    }

    // One file per shard, and one metadata stream per file: a trace describes
    // the objects *its own* thread saw loaded, so the shards are decoded
    // separately and merged afterwards rather than concatenated.
    std::vector<std::filesystem::path> files;
    for (const auto& e : std::filesystem::directory_iterator(argv[1])) {
        if (e.path().extension() == ".trace") {
            files.push_back(e.path());
        }
    }
    std::ranges::sort(files);
    if (files.empty()) {
        fprintf(stderr, "no *.trace files in %s\n", argv[1]);
        return 1;
    }

    // The objects the trace's source locations point into, as gathered beside
    // the traces by tools/gather-dsos. Nothing in the traced process writes
    // them: an address is read back against the object it is in, and finding
    // that object is the reader's job, not the writer's. $TRACE_DSO_DIR
    // overrides; without either, every location decodes as <unresolved 0x...>
    // and the rest of the trace is unaffected.
    const std::filesystem::path dso_dir = std::filesystem::path(argv[1]) / "dsos";
    trace::dso_directory dsos =
        std::getenv("TRACE_DSO_DIR") != nullptr || !std::filesystem::exists(dso_dir)
            ? trace::dso_directory()
            : trace::dso_directory(dso_dir.string());

    std::vector<entry> entries;
    for (const auto& file : files) {
        load_trace(file, entries, dsos);
        fmt::print("{}: {} records so far\n", file.string(), entries.size());
    }
    if (entries.empty()) {
        fprintf(stderr, "no records in %s\n", argv[1]);
        return 1;
    }

    // The analysis below walks `span` as a global timeline -- it was reading a
    // single thread's ring in file order -- so the shards have to be merged
    // into one before it can, and the timestamps are rdtsc from one machine,
    // which makes that meaningful.
    std::ranges::sort(entries, {}, &entry::ts);
    auto span = std::span<const entry>(entries);
    auto sorted = std::vector<entry>(span.begin(), span.end());
    std::ranges::sort(sorted, std::ranges::less(), [] (const auto &x) {return std::make_pair(x.query(), x.ts);});
#if 0
    for (const auto &x : sorted) {
        fmt::print("{:016x} {}\n", x.query(), x) ;
    }
#endif

    struct query {
        std::chrono::duration<double> latency;
        uint64_t id;
        std::chrono::duration<double> cputime;
        std::chrono::duration<double> iotime;
        std::chrono::duration<double> starvetime;
    };
    std::vector<query> queries;
    {
        size_t i = 0;
        while (i < sorted.size()) {
            while (i < sorted.size() && sorted[i].event != 1) {
                ++i;
            }
            if (i == sorted.size()) {
                break;
            }
            auto current_query = sorted[i].query();
            auto start = sorted[i].ts;
            while (i + 1 < sorted.size() && sorted[i + 1].query() == current_query) {
                ++i;
            }
            auto end = sorted[i].ts;
            auto time = std::chrono::duration<double, std::nano>(double(end - start) * MULTIPLIER);
            queries.push_back(query{time, current_query, {}, {}, {}});
            ++i;
        }
    }
    std::ranges::sort(queries, std::ranges::less(), [] (const auto &x) {return x.latency;});
#if 0
    for (const auto &x : queries) {
        fmt::print("{} {}\n", x.latency.count(), x.id) ;
    }
#endif

    {
        for (auto &x : queries) {
            auto id = x.id;
            auto sorted_range = std::ranges::equal_range(sorted, id, std::ranges::less(), [] (const auto& e) {return e.query();});
            //fmt::print("tsrange: {} {}\n", sorted_range.front().ts, sorted_range.back().ts);
            auto span_range = std::ranges::equal_range(span, 1, std::ranges::less(), [&sorted_range] (const auto& e) {return (e.ts >= sorted_range.front().ts) + (e.ts > sorted_range.back().ts);});

            uint64_t iostack = 0;
            bool cpu = true;
            uint64_t prev_ts = sorted_range.begin()->ts;
            uint64_t cputime = 0;
            uint64_t starvetime = 0;
            uint64_t iotime = 0;
            size_t i;
            //fmt::print("range: {} {}\n", span_range.begin() - span.begin(), span_range.end() - span.begin());
            for (i = span_range.begin() - span.begin(); i < size_t(span_range.end() - span.begin()); ++i) {
                //fmt::print("looping: {}\n", i);
                uint64_t dt = span[i].ts - prev_ts;
                if (iostack == 0 && !cpu) {
                    starvetime += dt;
                }
                if (cpu) {
                    cputime += dt;
                }
                if (iostack) {
                    iotime += dt;
                }
                if (span[i].query() == id) {
                    if (span[i].event != 0x5) {
                        cpu = true;
                    }
                    if (span[i].event == 0x4) {
                        iostack += 1;
                    } else if (span[i].event == 0x5) {
                        iostack -= 1;
                    }
                } else {
                    cpu = false;
                }
                prev_ts = span[i].ts;
            }
            auto conv = [] (uint64_t ticks) {
                return std::chrono::duration<double, std::nano>(ticks * MULTIPLIER);
            };
            x.iotime = conv(iotime);
            x.starvetime = conv(starvetime);
            x.cputime = conv(cputime);
            //fmt::print("cputime: {}", cputime);
        }
    }

    std::vector<double> xx;
    std::vector<double> yy;
    if (queries.size()) {
        for (int i = 0; i <= 1000; ++i) {
            double x = pow(100000.0, i/1000.0);
            size_t w = queries.size() - size_t(1.0 / x * queries.size());
            xx.push_back(x);
            yy.push_back(queries[std::clamp(w, size_t(0), queries.size() - 1)].latency.count());
        }
        for (size_t i = 0; i < xx.size(); ++i) {
            //fmt::print("{} {}\n", xx[i], yy[i]);
        }
    }

#if 0
    {
        double m_timerMul = 1.;

        std::atomic_signal_fence( std::memory_order_acq_rel );
        const auto t0 = std::chrono::high_resolution_clock::now();
        const auto r0 = rdtsc();
        std::atomic_signal_fence( std::memory_order_acq_rel );
        std::this_thread::sleep_for( std::chrono::milliseconds( 200 ) );
        std::atomic_signal_fence( std::memory_order_acq_rel );
        const auto t1 = std::chrono::high_resolution_clock::now();
        const auto r1 = rdtsc();
        std::atomic_signal_fence( std::memory_order_acq_rel );

        const auto dt = std::chrono::duration_cast<std::chrono::nanoseconds>( t1 - t0 ).count();
        const auto dr = r1 - r0;

        m_timerMul = double( dt ) / double( dr );
        fmt::print("dt: {}, dr: {}, MULT: {}\n", dt, dr, m_timerMul);
    }
#endif

    if (!SDL_Init(SDL_INIT_VIDEO | SDL_INIT_GAMEPAD)) {
        fprintf(stderr, "SDL_Init failed: %s\n", SDL_GetError());
        return 1;
    }

    // GL 3.0 + GLSL 130. SDL owns the window and context; the renderer
    // backend remains Dear ImGui's current OpenGL3 implementation.
    const char* glsl_version = "#version 130";
    SDL_GL_SetAttribute(SDL_GL_CONTEXT_MAJOR_VERSION, 3);
    SDL_GL_SetAttribute(SDL_GL_CONTEXT_MINOR_VERSION, 0);
    SDL_GL_SetAttribute(SDL_GL_CONTEXT_PROFILE_MASK, SDL_GL_CONTEXT_PROFILE_CORE);
    SDL_GL_SetAttribute(SDL_GL_DOUBLEBUFFER, 1);
    SDL_GL_SetAttribute(SDL_GL_DEPTH_SIZE, 24);
    SDL_GL_SetAttribute(SDL_GL_STENCIL_SIZE, 8);

    // Create window with graphics context
    SDL_Window* window = SDL_CreateWindow(
        "Latency analyzer", 1280, 720, SDL_WINDOW_OPENGL | SDL_WINDOW_RESIZABLE | SDL_WINDOW_HIGH_PIXEL_DENSITY);
    if (window == nullptr) {
        fprintf(stderr, "SDL_CreateWindow failed: %s\n", SDL_GetError());
        SDL_Quit();
        return 1;
    }
    SDL_GLContext gl_context = SDL_GL_CreateContext(window);
    if (gl_context == nullptr) {
        fprintf(stderr, "SDL_GL_CreateContext failed: %s\n", SDL_GetError());
        SDL_DestroyWindow(window);
        SDL_Quit();
        return 1;
    }
    SDL_GL_MakeCurrent(window, gl_context);
    SDL_GL_SetSwapInterval(1); // Enable vsync

    // Setup Dear ImGui context
    IMGUI_CHECKVERSION();
    ImGui::CreateContext();
    ImPlot::CreateContext();
    ImGuiIO& io = ImGui::GetIO(); (void)io;
    io.ConfigFlags |= ImGuiConfigFlags_NavEnableKeyboard;     // Enable Keyboard Controls
    io.ConfigFlags |= ImGuiConfigFlags_NavEnableGamepad;      // Enable Gamepad Controls
    io.ConfigFlags |= ImGuiConfigFlags_DockingEnable;         // Enable Docking

    // Setup Dear ImGui style
    ImGui::StyleColorsDark();
    //ImGui::StyleColorsLight();

    // Setup Platform/Renderer backends
    ImGui_ImplSDL3_InitForOpenGL(window, gl_context);
    ImGui_ImplOpenGL3_Init(glsl_version);

    // Load Fonts
    // - If no fonts are loaded, dear imgui will use the default font. You can also load multiple fonts and use ImGui::PushFont()/PopFont() to select them.
    // - AddFontFromFileTTF() will return the ImFont* so you can store it if you need to select the font among multiple.
    // - If the file cannot be loaded, the function will return a nullptr. Please handle those errors in your application (e.g. use an assertion, or display an error and quit).
    // - The fonts will be rasterized at a given size (w/ oversampling) and stored into a texture when calling ImFontAtlas::Build()/GetTexDataAsXXXX(), which ImGui_ImplXXXX_NewFrame below will call.
    // - Use '#define IMGUI_ENABLE_FREETYPE' in your imconfig file to use Freetype for higher quality font rendering.
    // - Read 'docs/FONTS.md' for more instructions and details.
    // - Remember that in C/C++ if you want to include a backslash \ in a string literal you need to write a double backslash \\ !
    // - Our Emscripten build process allows embedding fonts to be accessible at runtime from the "fonts/" folder. See Makefile.emscripten for details.
    //io.Fonts->AddFontDefault();
    //io.Fonts->AddFontFromFileTTF("c:\\Windows\\Fonts\\segoeui.ttf", 18.0f);
    //io.Fonts->AddFontFromFileTTF("../../misc/fonts/DroidSans.ttf", 16.0f);
    //io.Fonts->AddFontFromFileTTF("../../misc/fonts/Roboto-Medium.ttf", 16.0f);
    //io.Fonts->AddFontFromFileTTF("../../misc/fonts/Cousine-Regular.ttf", 15.0f);
    //ImFont* font = io.Fonts->AddFontFromFileTTF("c:\\Windows\\Fonts\\ArialUni.ttf", 18.0f, nullptr, io.Fonts->GetGlyphRangesJapanese());
    //IM_ASSERT(font != nullptr);

    // Our state
    bool show_demo_window = true;
    bool show_config_window = true;
    ImVec4 clear_color = ImVec4(0.45f, 0.55f, 0.60f, 1.00f);
    int log_task_threshold = 10000;
    log_cache log_cache_state;
    full_log_cache full_log_cache_state;

    // Main loop
    bool done = false;
    while (!done)
    {
        // Poll and handle events (inputs, window resize, etc.)
        // You can read the io.WantCaptureMouse, io.WantCaptureKeyboard flags to tell if dear imgui wants to use your inputs.
        // - When io.WantCaptureMouse is true, do not dispatch mouse input data to your main application, or clear/overwrite your copy of the mouse data.
        // - When io.WantCaptureKeyboard is true, do not dispatch keyboard input data to your main application, or clear/overwrite your copy of the keyboard data.
        // Generally you may always pass all inputs to dear imgui, and hide them from your application based on those two flags.
        SDL_Event event;
        while (SDL_PollEvent(&event)) {
            ImGui_ImplSDL3_ProcessEvent(&event);
            if (event.type == SDL_EVENT_QUIT ||
                (event.type == SDL_EVENT_WINDOW_CLOSE_REQUESTED &&
                 event.window.windowID == SDL_GetWindowID(window))) {
                done = true;
            }
        }

        // Start the Dear ImGui frame
        ImGui_ImplOpenGL3_NewFrame();
        ImGui_ImplSDL3_NewFrame();
        ImGui::NewFrame();

        if (ImGui::BeginMainMenuBar()) {
            if (ImGui::BeginMenu("View")) {
                if (ImGui::BeginMenu("Dockers")) {
                    ImGui::MenuItem("Config", nullptr, &show_config_window);
                    ImGui::EndMenu();
                }
                ImGui::EndMenu();
            }
            ImGui::EndMainMenuBar();
        }

        ImGui::DockSpaceOverViewport();

        if (show_config_window) {
            ImGui::Begin("Config", &show_config_window);
            ImGui::InputInt("Log task threshold", &log_task_threshold);
            log_task_threshold = std::max(log_task_threshold, 0);
            ImGui::End();
        }

        // 1. Show the big demo window (Most of the sample code is in ImGui::ShowDemoWindow()! You can browse its code to learn more about Dear ImGui!).
        if (show_demo_window) {
            // ImGui::ShowDemoWindow(&show_demo_window);
        }

#if 0
        // 2. Show a simple window that we create ourselves. We use a Begin/End pair to create a named window.
        {
            static float f = 0.0f;
            static int counter = 0;

            ImGui::Begin("Hello, world!");                          // Create a window called "Hello, world!" and append into it.

            ImGui::Text("This is some useful text.");               // Display some text (you can use a format strings too)
            ImGui::Checkbox("Demo Window", &show_demo_window);      // Edit bools storing our window open/close state
            ImGui::Checkbox("Another Window", &show_another_window);

            ImGui::SliderFloat("float", &f, 0.0f, 1.0f);            // Edit 1 float using a slider from 0.0f to 1.0f
            ImGui::ColorEdit3("clear color", (float*)&clear_color); // Edit 3 floats representing a color

            if (ImGui::Button("Button"))                            // Buttons return true when clicked (most widgets return true when edited/activated)
                counter++;
            ImGui::SameLine();
            ImGui::Text("counter = %d", counter);

            ImGui::Text("Application average %.3f ms/frame (%.1f FPS)", 1000.0f / io.Framerate, io.Framerate);

            {
                static bool animate = true;
                ImGui::Checkbox("Animate", &animate);

                // Plot as lines and plot as histogram
                //IMGUI_DEMO_MARKER("Widgets/Plotting/PlotLines, PlotHistogram");
                static float arr[] = { 0.6f, 0.1f, 1.0f, 0.5f, 0.92f, 0.1f, 0.2f };
                ImGui::PlotLines("Frame Times", arr, IM_ARRAYSIZE(arr));
                ImGui::PlotHistogram("Histogram", arr, IM_ARRAYSIZE(arr), 0, NULL, 0.0f, 1.0f, ImVec2(0, 80.0f));

                // Fill an array of contiguous float values to plot
                // Tip: If your float aren't contiguous but part of a structure, you can pass a pointer to your first float
                // and the sizeof() of your structure in the "stride" parameter.
                static float values[90] = {};
                static int values_offset = 0;
                static double refresh_time = 0.0;
                if (!animate || refresh_time == 0.0)
                    refresh_time = ImGui::GetTime();
                while (refresh_time < ImGui::GetTime()) // Create data at fixed 60 Hz rate for the demo
                {
                    static float phase = 0.0f;
                    values[values_offset] = cosf(phase);
                    values_offset = (values_offset + 1) % IM_ARRAYSIZE(values);
                    phase += 0.10f * values_offset;
                    refresh_time += 1.0f / 60.0f;
                }

                // Plots can display overlay texts
                // (in this example, we will display an average value)
                {
                    float average = 0.0f;
                    for (int n = 0; n < IM_ARRAYSIZE(values); n++)
                        average += values[n];
                    average /= (float)IM_ARRAYSIZE(values);
                    char overlay[32];
                    sprintf(overlay, "avg %f", average);
                    ImGui::PlotLines("Lines", values, IM_ARRAYSIZE(values), values_offset, overlay, -1.0f, 1.0f, ImVec2(0, 80.0f));
                }

                // Use functions to generate output
                // FIXME: This is rather awkward because current plot API only pass in indices.
                // We probably want an API passing floats and user provide sample rate/count.
                struct Funcs
                {
                    static float Sin(void*, int i) { return sinf(i * 0.1f); }
                    static float Saw(void*, int i) { return (i & 1) ? 1.0f : -1.0f; }
                };
                static int func_type = 0, display_count = 70;
                ImGui::Separator();
                ImGui::SetNextItemWidth(ImGui::GetFontSize() * 8);
                ImGui::Combo("func", &func_type, "Sin\0Saw\0");
                ImGui::SameLine();
                ImGui::SliderInt("Sample count", &display_count, 1, 400);
                float (*func)(void*, int) = (func_type == 0) ? Funcs::Sin : Funcs::Saw;
                ImGui::PlotLines("Lines", func, NULL, display_count, 0, NULL, -1.0f, 1.0f, ImVec2(0, 80));
                ImGui::PlotHistogram("Histogram", func, NULL, display_count, 0, NULL, -1.0f, 1.0f, ImVec2(0, 80));
                ImGui::Separator();

                // Animate a simple progress bar
                //IMGUI_DEMO_MARKER("Widgets/Plotting/ProgressBar");
                static float progress = 0.0f, progress_dir = 1.0f;
                if (animate)
                {
                    progress += progress_dir * 0.4f * ImGui::GetIO().DeltaTime;
                    if (progress >= +1.1f) { progress = +1.1f; progress_dir *= -1.0f; }
                    if (progress <= -0.1f) { progress = -0.1f; progress_dir *= -1.0f; }
                }

                // Typically we would use ImVec2(-1.0f,0.0f) or ImVec2(-FLT_MIN,0.0f) to use all available width,
                // or ImVec2(width,0.0f) for a specified width. ImVec2(0.0f,0.0f) uses ItemWidth.
                ImGui::ProgressBar(progress, ImVec2(0.0f, 0.0f));
                ImGui::SameLine(0.0f, ImGui::GetStyle().ItemInnerSpacing.x);
                ImGui::Text("Progress Bar");

                float progress_saturated = std::clamp(progress, 0.0f, 1.0f);
                char buf[32];
                sprintf(buf, "%d/%d", (int)(progress_saturated * 1753), 1753);
                ImGui::ProgressBar(progress, ImVec2(0.f, 0.f), buf);
            }
            ImGui::End();
        }
#endif

        //ImPlot::ShowDemoWindow();

#if 0
        // 3. Show another simple window.
        if (show_another_window)
        {
            ImGui::Begin("Another Window", &show_another_window);   // Pass a pointer to our bool variable (the window will have a closing button that will clear the bool when clicked)
            ImGui::Text("Hello from another window!");
            if (ImGui::Button("Close Me"))
                show_another_window = false;
            ImGui::End();
        }
#endif

        if (!queries.empty()) {
            static size_t chosen_one = -1;
            static bool just_chosen = true;
            static size_t chosen_unfull = -1;
            static bool just_chosen_unfull = true;
            static uint64_t id_log = queries[0].id;
            {
            ImGui::Begin("Graph");
            static double line_x;
            static size_t w = 0;
            static uint64_t id_full_log = id_log;
            static double rect[] = {100.0, 0.001, 141.2, 0.003};

            if (ImPlot::BeginPlot("HdrHistogram", ImVec2(-1,0))) {
                ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_Lock, ImPlotAxisFlags_Lock);
                ImPlot::SetupAxisScale(ImAxis_X1, ImPlotScale_Log10);
                ImPlot::SetupAxisScale(ImAxis_Y1, ImPlotScale_Log10);
                ImPlot::SetupAxesLimits(1, 100000, 0.0001, queries.back().latency.count());
                ImPlot::PlotLine("Latency", xx.data(), yy.data(), 1001);

                if (ImPlot::IsPlotHovered() && ImGui::IsMouseDown(0)) {
                    ImPlotPoint pt = ImPlot::GetPlotMousePos();
                    line_x = std::clamp(pt.x, 1.0, 100000.0);
                    w = std::clamp(queries.size() - size_t(1.0 / line_x * queries.size()), size_t(0), size_t(queries.size() - 1));
                    id_log = queries[w].id;
                    id_full_log = id_log;
                }
                ImPlotDragToolFlags flags = ImPlotDragToolFlags_NoCursors | ImPlotDragToolFlags_NoFit | ImPlotDragToolFlags_NoInputs;
                ImPlot::DragLineX(0, &line_x, ImVec4(1,1,1,1), 1, flags);

                rect[1] = 0.0001;
                rect[3] = 0.001;
                ImPlot::DragRect(0,&rect[0],&rect[1],&rect[2],&rect[3],ImVec4(1,0,1,1), ImPlotDragToolFlags_Delayed);

                ImPlot::EndPlot();
            }

            ImGui::End();

            ImGui::Begin("TimeDist");
            {
                static size_t w1g = -1;
                static size_t w2g = -1;
                size_t w1 = std::clamp(queries.size() - size_t(1.0 / rect[0] * queries.size()), size_t(0), size_t(queries.size() - 1));
                size_t w2 = std::clamp(queries.size() - size_t(1.0 / rect[2] * queries.size()), size_t(0), size_t(queries.size() - 1));
                using t = std::chrono::duration<double>;
                static std::vector<double> plot_x = std::invoke([&] {
                    std::vector<double> v;
                    for (int i = 0; i < 1024; ++i) {
                        v.push_back(i * (1.0/1024));
                    }
                    return v;
                });
                static std::vector<double> iotimes_y, cputimes_y, latencies_y, starvetimes_y;
                static t avgiotime, avgcputime, avgstarvetime, avglatency;

                if (w1 != w1g || w2 != w2g) {
                    w1g = w1;
                    w2g = w2;
                    avgiotime = avgcputime = avglatency = avgstarvetime = t::zero();
                    std::vector<t> iotimes, cputimes, latencies, starvetimes;
                    for (size_t i = w1; i <= w2; ++i) {
                        avgiotime += queries[i].iotime / (w2 - w1 + 1);
                        avgcputime += queries[i].cputime / (w2 - w1 + 1);
                        avgstarvetime += queries[i].starvetime / (w2 - w1 + 1);
                        avglatency += queries[i].latency / (w2 - w1 + 1);

                        iotimes.push_back(queries[i].iotime);
                        cputimes.push_back(queries[i].cputime);
                        starvetimes.push_back(queries[i].starvetime);
                        latencies.push_back(queries[i].latency);
                    }
                    std::ranges::sort(iotimes);
                    std::ranges::sort(cputimes);
                    std::ranges::sort(starvetimes);
                    std::ranges::sort(latencies);

                    auto sample = [&] (std::vector<t>& vec) {
                        auto res = std::vector<double>();
                        if (vec.empty()) {
                            return res;
                        }
                        for (const auto& p : plot_x) {
                            size_t ww = (vec.size() - 1) * p;
                            res.push_back(std::chrono::duration<double, std::milli>(vec[ww]).count());
                        }
                        return res;
                    };
                    iotimes_y = sample(iotimes);
                    starvetimes_y = sample(starvetimes);
                    cputimes_y = sample(cputimes);
                    latencies_y = sample(latencies);
                }

                ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "CPU", std::chrono::duration<double, std::milli>(avgcputime).count()).c_str());
                ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "STARVE", std::chrono::duration<double, std::milli>(avgstarvetime).count()).c_str());
                ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "IO", std::chrono::duration<double, std::milli>(avgiotime).count()).c_str());
                ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "TOTAL", std::chrono::duration<double, std::milli>(avglatency).count()).c_str());

                if (ImPlot::BeginSubplots("My Subplot",2,2,ImVec2(-1, -1))) {
                    if (ImPlot::BeginPlot("iotime cdf", ImVec2(-1,0))) {
                        ImPlot::SetupAxes(NULL,NULL,0,ImPlotAxisFlags_AutoFit|ImPlotAxisFlags_RangeFit);
                        ImPlot::PlotLine("iotime cdf", plot_x.data(), iotimes_y.data(), iotimes_y.size());
                        ImPlot::EndPlot();
                    }
                    if (ImPlot::BeginPlot("starvetime cdf", ImVec2(-1,0))) {
                        ImPlot::SetupAxes(NULL,NULL,0,ImPlotAxisFlags_AutoFit|ImPlotAxisFlags_RangeFit);
                        ImPlot::PlotLine("starvetime cdf", plot_x.data(), starvetimes_y.data(), starvetimes_y.size());
                        ImPlot::EndPlot();
                    }
                    if (ImPlot::BeginPlot("cputime cdf", ImVec2(-1,0))) {
                        ImPlot::SetupAxes(NULL,NULL,0,ImPlotAxisFlags_AutoFit|ImPlotAxisFlags_RangeFit);
                        ImPlot::PlotLine("cputime cdf", plot_x.data(), cputimes_y.data(), cputimes_y.size());
                        ImPlot::EndPlot();
                    }
                    if (ImPlot::BeginPlot("latency cdf", ImVec2(-1,0))) {
                        ImPlot::SetupAxes(NULL,NULL,0,ImPlotAxisFlags_AutoFit|ImPlotAxisFlags_RangeFit);
                        ImPlot::PlotLine("latency cdf", plot_x.data(), latencies_y.data(), latencies_y.size());
                        ImPlot::EndPlot();
                    }
                    ImPlot::EndSubplots();
                }
            }
            ImGui::End();

            {
                ImGui::Begin("Log");
                ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "CPU", std::chrono::duration<double, std::milli>(queries[w].cputime).count()).c_str());
                ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "STARVE", std::chrono::duration<double, std::milli>(queries[w].starvetime).count()).c_str());
                ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "IO", std::chrono::duration<double, std::milli>(queries[w].iotime).count()).c_str());
                ImGui::Text("%s", fmt::format("{:10s} {:12.9f}", "TOTAL", std::chrono::duration<double, std::milli>(queries[w].latency).count()).c_str());
                update_log_cache(log_cache_state, id_log, log_task_threshold, sorted);
                if (log_cache_state.item_count > static_cast<size_t>(log_task_threshold)) {
                    render_truncation_warning(log_cache_state.item_count, log_task_threshold, false);
                }
                if (ImGui::BeginChild("Log entries", ImVec2(0, 0), ImGuiChildFlags_None, ImGuiWindowFlags_HorizontalScrollbar)) {
                    {
                        static size_t selected = -1;
                        ImGuiListClipper clipper;
                        clipper.Begin(static_cast<int>(log_cache_state.lines.size()));
                        if (chosen_unfull >= log_cache_state.source_begin &&
                            chosen_unfull < log_cache_state.source_begin + log_cache_state.lines.size()) {
                            clipper.IncludeItemByIndex(static_cast<int>(chosen_unfull - log_cache_state.source_begin));
                        }
                        while (clipper.Step()) {
                            for (int visible_index = clipper.DisplayStart;
                                 visible_index < clipper.DisplayEnd; ++visible_index) {
                                const auto& line = log_cache_state.lines[visible_index];
                                const size_t i = line.source_index;
                                const bool selected_in_range =
                                    selected >= log_cache_state.source_begin &&
                                    selected < log_cache_state.source_begin + log_cache_state.lines.size() &&
                                    selected < sorted.size();
                                const bool highlighted =
                                    selected_in_range &&
                                    (line.record.event == 0x4 || line.record.event == 0x5) &&
                                    (line.record.arg == sorted[selected].arg) &&
                                    (sorted[selected].event == 0x4 || sorted[selected].event == 0x5);
                                if (i == chosen_unfull) {
                                    if (just_chosen_unfull) {
                                        just_chosen_unfull = false;
                                        ImGui::SetScrollHereY();
                                    }
                                    ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(1.f, 0.f, 0.0f, 1.f));
                                }
                                if (ImGui::Selectable(line.text.c_str(), highlighted)) {
                                    selected = highlighted ? size_t(-1) : i;
                                }
                                if (i == chosen_unfull) {
                                    ImGui::PopStyleColor();
                                }
                            }
                        }
                    }
                }
                ImGui::EndChild();
                ImGui::End();
            }
#if 1
            {
                ImGui::Begin("Full log");
                update_full_log_cache(full_log_cache_state, id_full_log, log_task_threshold, sorted, span);
                if (full_log_cache_state.task_count > static_cast<size_t>(log_task_threshold)) {
                    render_truncation_warning(full_log_cache_state.task_count, log_task_threshold, true);
                }
                if (ImGui::BeginChild("Full log entries", ImVec2(0, 0), ImGuiChildFlags_None, ImGuiWindowFlags_HorizontalScrollbar)) {
                    {
                        static size_t selected = 0;
                        ImGuiListClipper clipper;
                        clipper.Begin(static_cast<int>(full_log_cache_state.lines.size()));
                        if (chosen_one >= full_log_cache_state.source_begin &&
                            chosen_one < full_log_cache_state.source_begin + full_log_cache_state.lines.size()) {
                            clipper.IncludeItemByIndex(static_cast<int>(chosen_one - full_log_cache_state.source_begin));
                        }
                        while (clipper.Step()) {
                            for (int visible_index = clipper.DisplayStart;
                                 visible_index < clipper.DisplayEnd; ++visible_index) {
                                const auto& line = full_log_cache_state.lines[visible_index];
                                const size_t i = line.source_index;
                                const bool selected_in_range =
                                    selected >= full_log_cache_state.source_begin &&
                                    selected < full_log_cache_state.source_begin + full_log_cache_state.lines.size() &&
                                    selected < span.size();
                                const bool highlighted =
                                    selected_in_range && line.record.query() == id_log;
                                const bool is_active = line.record.query() == id_full_log;
                                if (is_active) {
                                    ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(0.f, 1.f, 0.24f, 1.f));
                                }
                                if (i == chosen_one) {
                                    if (just_chosen) {
                                        just_chosen = false;
                                        ImGui::SetScrollHereY();
                                    }
                                    ImGui::PushStyleColor(ImGuiCol_Text, ImVec4(1.f, 0.f, 0.0f, 1.f));
                                }
                                if (ImGui::Selectable(line.text.c_str(), highlighted)) {
                                    auto x = line.record.query();
                                    if (x) {
                                        id_log = x;
                                    }
                                    selected = highlighted ? size_t(-1) : i;
                                }
                                if (i == chosen_one) {
                                    ImGui::PopStyleColor();
                                }
                                if (is_active) {
                                    ImGui::PopStyleColor();
                                }
                            }
                        }
                    }
                }
                ImGui::EndChild();
                ImGui::End();
            }
#endif
            {
                ImGui::Begin("Full log plot");
                if (full_log_cache_state.task_count > static_cast<size_t>(log_task_threshold)) {
                    render_truncation_warning(full_log_cache_state.task_count, log_task_threshold, true);
                }
                if (ImPlot::BeginPlot("Full log plot", ImVec2(-1, 100), ImPlotFlags_NoTitle)) {
                    static uint64_t prev_id;
                    auto flag = prev_id == id_full_log ? ImPlotCond_Once : ImPlotCond_Always;
                    prev_id = id_full_log;

                    const int64_t start_ts = full_log_cache_state.start_ts;
                    const int64_t end_ts = full_log_cache_state.end_ts;
                    ImPlot::SetupAxes(nullptr, nullptr, ImPlotAxisFlags_NoGridLines, ImPlotAxisFlags_Lock | ImPlotAxisFlags_NoDecorations);
                    ImPlot::SetupAxisLimitsConstraints(ImAxis_X1, 0, double(end_ts - start_ts)*MULTIPLIER/1e6);
                    ImPlot::SetupAxesLimits(0, double(end_ts - start_ts)*MULTIPLIER/1e6, 0, 1, flag);
                    ImPlot::PushPlotClipRect();

                    for (const auto& item : full_log_cache_state.plot_items) {
                        ImVec2 rmin = ImPlot::PlotToPixels(item.min);
                        ImVec2 rmax = ImPlot::PlotToPixels(item.max);
                        if (item.draw_line) {
                            ImVec2 line_end = ImPlot::PlotToPixels(item.line_end);
                            ImPlot::GetPlotDrawList()->AddLine(rmin, line_end, IM_COL32(0,128,0,255));
                        }
                        ImPlot::GetPlotDrawList()->AddRectFilled(rmin, rmax, item.color);
                    }
                    ImPlot::PopPlotClipRect();

                    if (ImPlot::IsPlotHovered() && ImGui::IsMouseDown(0)) {
                        ImPlotPoint pt = ImPlot::GetPlotMousePos();
                        uint64_t ts = start_ts + pt.x * 1e6 / MULTIPLIER;
                        chosen_one = std::ranges::lower_bound(span, ts, std::ranges::less(), [] (const auto& e) {return e.ts;}) - span.begin() - 1;
                        chosen_one = std::clamp(chosen_one, size_t(0), span.size() - 1);
                        just_chosen = true;
                        chosen_unfull = std::ranges::lower_bound(sorted, std::make_pair<uint64_t, uint64_t>(uint64_t(id_log), uint64_t(ts)), std::ranges::less(), [] (const auto& e) {return std::make_pair<uint64_t, uint64_t>(e.query(), e.ts);}) - sorted.begin() - 1;
                        chosen_unfull = std::clamp(chosen_unfull, size_t(0), sorted.size() - 1);
                        just_chosen_unfull = true;
                    }
                    ImPlot::EndPlot();
                }
                ImGui::End();
            }

            }
        } else {
            ImGui::Begin("Trace viewer");
            ImGui::Text("No query records found in %s.", argv[1]);
            ImGui::End();
        }

        // Rendering
        ImGui::Render();
        int display_w, display_h;
        SDL_GetWindowSizeInPixels(window, &display_w, &display_h);
        glViewport(0, 0, display_w, display_h);
        glClearColor(clear_color.x * clear_color.w, clear_color.y * clear_color.w, clear_color.z * clear_color.w, clear_color.w);
        glClear(GL_COLOR_BUFFER_BIT);
        ImGui_ImplOpenGL3_RenderDrawData(ImGui::GetDrawData());

        SDL_GL_SwapWindow(window);
    }

    // Cleanup
    ImGui_ImplOpenGL3_Shutdown();
    ImGui_ImplSDL3_Shutdown();
    ImPlot::DestroyContext();
    ImGui::DestroyContext();

    SDL_GL_DestroyContext(gl_context);
    SDL_DestroyWindow(window);
    SDL_Quit();

    return 0;
}
