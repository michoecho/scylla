# cpp_template

```sh
cmake --preset Debug                      # configure
cmake --build --preset Debug              # build
ctest --preset DebugTest                  # test
cmake --preset Debug -DENABLE_TEST_COVERAGE=YES -B out/build/Cov && cmake --build out/build/Cov && ctest --test-dir out/build/Cov  # coverage on any preset
./tools/merge-coverage out/build/Cov      # -> out/build/Cov/coverage/total/total/index.html
```
