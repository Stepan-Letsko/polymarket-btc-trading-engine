#include <chrono>
#include <string>
#include <ixwebsocket/IXHttpClient.h>
#include "simdjson.h"
#include <iostream>
#include "get_market.h"

std::string get_current_slug() {

    auto now = std::chrono::system_clock::now();
    int64_t now_secs = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    int64_t rounded = (now_secs / 300) * 300;  // round down to nearest 5 minutes
    return "btc-updown-5m-" + std::to_string(rounded);
}

Market get_current_market() {

    Market market_result;

    ix::HttpClient httpClient;
    std::string url = "https://gamma-api.polymarket.com/events?slug=" + get_current_slug();
    auto args = httpClient.createRequest(url);
    ix::HttpResponsePtr response = httpClient.get(url, args);

    if (response->statusCode != 200) {
        std::cout << "Request failed, status code: " << response->statusCode << std::endl;
        return market_result;
    }

    simdjson::ondemand::parser parser;
    simdjson::padded_string padded = simdjson::padded_string(response->body);
    auto doc = parser.iterate(padded);

    auto events = doc.get_array();
    auto event = events.at(0);
    auto markets = event["markets"];
    auto market = markets.at(0);

    std::string_view clob_token_ids_raw = market["clobTokenIds"];

    simdjson::ondemand::parser token_parser;
    simdjson::padded_string token_padded = simdjson::padded_string(clob_token_ids_raw);
    auto token_doc = token_parser.iterate(token_padded);

    auto token_array = token_doc.get_array();
    auto it = token_array.begin();
    std::string_view token_up = *it;
    ++it;
    std::string_view token_down = *it;

    market_result.token_up = std::string(token_up);
    market_result.token_down = std::string(token_down);

    return market_result;
}

