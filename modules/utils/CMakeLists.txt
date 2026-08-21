add_module(utils
    SOURCES clock.cc)

target_compile_options(utils PRIVATE
    -Wall
    -Wextra
)
