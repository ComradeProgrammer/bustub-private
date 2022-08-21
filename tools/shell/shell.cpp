#include <iostream>
#include "execution/execution_engine.h"

int main() {
  using namespace bustub;

  std::string query;

  while (true) {
    std::cout << "> ";
    std::getline(std::cin, query);
    if (!std::cin) break;
    std::cout << query << std::endl;
  }
  return 0;
}
