There are two separate build systems here: cmake (CMakeLists.txt) and configure.py. This project is built with configure.py, NOT cmake. When adding a new test target (or any new build target), register it in configure.py — do not edit CMakeLists.txt.
Never run configure.py directly without asking, or without being explicitly instructed to do it. You will likely reset the user's preferred compile flags this way. You should never have any need to run configure.py, though. After changing configure.py, you can just run ninja and it will automatically pick up the changes.   

Full builds are done with `ninja dev-build`.
Full unit tests are run with `ninja dev-build && ./test.py test/boost`.
There are other tests beyond `test/boost` but they are expensive and you should only run them when clearly asked.
To run a particular C++ test, you first need to build it like `ninja build/dev/test/boost/name_of_test_executable`, then run it with `pytest test/boost/my_test_name.cc`. Some tests belong to executable test/boost/combined_tests, other have their own executables, named after the source file.
To run a particular Python test, you first need to build `ninja build/dev/scylla`, then run `pytest test/suite_name/test_file.py`. If you want to run a particular test case, figure out on your own.

If only object storage tests are failing, it might be caused by leftover metadata in the container. In that case, running `rm -rf /run/user/1000/containers /run/user/1000/libpod/tmp` might fix the problem.

Test executables are stripped by default to conserve disk space.
For every test, there's a separate ninja target for a non-stripped executable (same path, with a `_g` added at the end). If you want to have debug symbols in a test, build that target and copy it over the stripped executable.

Do not run commands in the background and do not use the Monitor tool. Run builds, tests, and other commands in the foreground and wait for them to finish before continuing. Do not narrate or do other work while a command is running.
