#include "get_market.h"
#include <cstdlib>
#include <iostream>

// Usage:
//   ./get_market                 the window running right now
//   ./get_market <boundary_ts>   a specific window, e.g. a past or
//                                not-yet-listed one to exercise the
//                                failure path
int main(int argc, char* argv[]) {
	Market market = (argc > 1)
		? get_market_for_boundary(std::strtoll(argv[1], nullptr, 10))
		: get_current_market();

	if (!market.valid()) {
		std::cerr << "no valid market returned" << std::endl;
		return 1;
	}

	std::cout << "token up: " << market.token_up << std::endl;
	std::cout << "token down: " << market.token_down << std::endl;
	std::cout << "condition id: " << market.condition_id << std::endl;

	return 0;
}
