#include "get_market.h"
#include <iostream>

int main() {
	Market market = get_current_market();
	std::cout << "token up: " << market.token_up << std::endl;
	std::cout << "token down: " << market.token_down << std::endl;

return 0;
}
