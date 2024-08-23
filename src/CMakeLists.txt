add_library(Lib trie.cc)
target_compile_options(Lib PRIVATE -coverage)
target_link_options(Lib PRIVATE -coverage)
