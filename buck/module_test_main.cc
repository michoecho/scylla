// The main() linked into every module test executable.

#include <cstring>
#include <string>
#include <vector>

#include "module_run.h"

#ifndef MODULE_NAME
#error "MODULE_NAME must be defined by the Buck2 rule"
#endif
#ifndef MODULE_SOURCE_DIR
#error "MODULE_SOURCE_DIR must be defined by the Buck2 rule"
#endif

int main(int argc, char** argv) {
    bool run_all = false;
    std::vector<char*> forwarded;
    forwarded.push_back(argv[0]);
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--all") == 0)
            run_all = true;
        else
            forwarded.push_back(argv[i]);
    }

    run::Command command = run::classify(static_cast<int>(forwarded.size()),
                                         forwarded.data(),
                                         /*default_to_test=*/true);

    std::vector<std::string> preset;
    if (!run_all)
        preset.push_back(std::string("--source-file=*") + MODULE_SOURCE_DIR + "/*");

    return run::execute(argv[0], command, preset);
}
