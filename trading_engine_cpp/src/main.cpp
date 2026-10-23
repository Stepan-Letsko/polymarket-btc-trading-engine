#include "simdjson.h"
#include <iostream>

int main() {
    std::string sample = R"({"e": "depthUpdate", "E": 1790270400034, "s": "BTCUSDT"})";
    simdjson::padded_string padded = simdjson::padded_string(sample);

    simdjson::ondemand::parser parser;
    auto doc = parser.iterate(padded);

    int64_t event_time = doc["E"];
    std::cout << "Parsed event_time: " << event_time << std::endl;

    return 0;
}
