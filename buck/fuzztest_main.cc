// A GoogleTest-free entry point for FuzzTest.
//
// FuzzTest ships no such main. Both `fuzztest_gtest_main` and `llvm_fuzzer_main`
// route through `init_fuzztest`, which depends on `googletest_adaptor`. But the
// framework proper does not depend on GoogleTest: `fuzztest_core` reaches the
// registration, registry, domain and runtime machinery without it, and upstream
// says as much in MODULE.bazel and doc/quickstart-bazel.md -- only that using it
// that way is "beyond the scope of this tutorial". So the driver is the one
// missing piece, and this is it.
//
// The seam is `ForEachTest` in fuzztest/internal/registry.h, the same one
// googletest_adaptor.cc walks to register every FUZZ_TEST as a gtest case. There
// each test becomes a TEST; here each becomes a name the command line selects.
//
// Note that registry.h and runtime.h are internal headers with no stability
// guarantee. That is the cost of the unpaved path, and the reason this file
// stays confined to `ForEachTest` / `RunInFuzzingMode` rather than reaching
// deeper into the runtime.

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "absl/time/time.h"
#include "fuzztest/internal/configuration.h"
#include "fuzztest/internal/registry.h"
#include "fuzztest/internal/runtime.h"

namespace {

std::vector<std::string> RegisteredTests() {
  std::vector<std::string> names;
  fuzztest::internal::ForEachTest([&](fuzztest::internal::FuzzTest& test) {
    names.push_back(test.full_name());
  });
  return names;
}

int Usage(const char* argv0) {
  std::fprintf(stderr,
               "usage: %s [--list] [--seconds=N] [<suite>.<test>]\n"
               "\n"
               "  --list       print the registered fuzz tests and exit\n"
               "  --seconds=N  stop fuzzing after N seconds (default 30)\n"
               "\n"
               "With no test named, the only registered test is run; if there\n"
               "is more than one, a name is required.\n",
               argv0);
  return 2;
}

}  // namespace

int main(int argc, char** argv) {
  // Turns a crash into a FuzzTest report -- the stack trace and the reproducer
  // -- rather than a bare signal. ASan installs its own handler for the memory
  // errors it detects; this covers the rest.
  fuzztest::internal::InstallSignalHandlers(stderr);

  std::string selected;
  int seconds = 30;

  for (int i = 1; i < argc; ++i) {
    const std::string_view arg = argv[i];
    if (arg == "--list") {
      for (const std::string& name : RegisteredTests()) {
        std::printf("%s\n", name.c_str());
      }
      return 0;
    } else if (arg.rfind("--seconds=", 0) == 0) {
      seconds = std::atoi(arg.substr(std::strlen("--seconds=")).data());
    } else if (arg.rfind("-", 0) == 0) {
      return Usage(argv[0]);
    } else {
      selected = std::string(arg);
    }
  }

  const std::vector<std::string> names = RegisteredTests();
  if (names.empty()) {
    std::fprintf(stderr, "[!] no FUZZ_TESTs are registered in this binary\n");
    return 2;
  }
  if (selected.empty()) {
    if (names.size() != 1) {
      std::fprintf(stderr, "[!] %zu fuzz tests registered; name one:\n",
                   names.size());
      for (const std::string& name : names) {
        std::fprintf(stderr, "      %s\n", name.c_str());
      }
      return 2;
    }
    selected = names.front();
  }

  // Every field of Configuration has a default, so only what this driver
  // actually varies is set. An empty `corpus_database` means findings are
  // reported but not persisted, which is what we want for a one-shot run.
  fuzztest::internal::Configuration configuration;
  configuration.binary_identifier = argv[0];
  configuration.fuzz_tests = names;
  configuration.fuzz_tests_in_current_shard = {selected};
  configuration.time_limit = absl::Seconds(seconds);
  configuration.time_budget_type = fuzztest::internal::TimeBudgetType::kPerTest;

  bool found = false;
  bool ok = false;
  fuzztest::internal::ForEachTest([&](fuzztest::internal::FuzzTest& test) {
    if (found || test.full_name() != selected) return;
    found = true;
    // Returns false when the fuzzer found a failure. A crash that ASan or the
    // signal handler catches never returns here at all -- the process dies
    // inside, after the report is written.
    ok = test.make()->RunInFuzzingMode(/*argc=*/nullptr, /*argv=*/nullptr,
                                       configuration);
  });

  if (!found) {
    std::fprintf(stderr, "[!] no fuzz test named %s\n", selected.c_str());
    return 2;
  }
  return ok ? 0 : 1;
}
