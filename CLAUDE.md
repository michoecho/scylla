Full builds are done with `ninja dev-build`.
Full tests are run with `ninja dev-test`.
To run a particular C++ test, you first need to build it like `ninja build/dev/test/boost/name_of_test_executable`, then run it with `pytest test/boost/my_test_name.cc`. Some tests belong to executable test/boost/combined_tests, other have their own executables, named after the source file.
To run a particular Python test, you first need to build `ninja build/dev/scylla`, then run `pytest test/suite_name/test_file.py`. If you want to run a particular test case, figure out on your own.
