#include <chrono>
#include <string>
#include <ixwebsocket/IXHttpClient.h>
#include "simdjson.h"
#include <iostream>
#include "get_market.h"

int64_t current_boundary() {
    auto now = std::chrono::system_clock::now();
    int64_t now_secs = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    return (now_secs / 300) * 300;  // round down to nearest 5 minutes
}

// static = private to this file. Takes the slug rather than deciding it,
// so callers can ask for a window other than the one running right now
// (needed to pre-fetch the upcoming market before its window starts).
static Market fetch_market(const std::string& slug) {

    Market market_result;

    ix::HttpClient httpClient;
    std::string url = "https://gamma-api.polymarket.com/events?slug=" + slug;
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

    std::string_view condition_id = market["conditionId"];
    market_result.condition_id = std::string(condition_id);

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

Market get_market_for_boundary(int64_t boundary_ts) {
    return fetch_market("btc-updown-5m-" + std::to_string(boundary_ts));
}

Market get_current_market() {
    return get_market_for_boundary(current_boundary());
}
