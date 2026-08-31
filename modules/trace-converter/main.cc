#include "converter.h"

#include <exception>
#include <iostream>

int main(int argc, char **argv) {
  if (argc != 2 && argc != 3) {
    std::cerr << "usage: " << argv[0] << " SNAPSHOT-DIR [OUTPUT]\n"
              << "  OUTPUT defaults to scylla.perfetto-trace in the current "
                 "directory\n";
    return 2;
  }

  const std::filesystem::path output =
      argc == 3 ? argv[2] : std::filesystem::path("scylla.perfetto-trace");
  try {
    const std::size_t count = scylla_trace::convert_snapshot(argv[1], output);
    std::cout << "wrote " << output << " (" << count << " Scylla events)\n";
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
  return 0;
}
