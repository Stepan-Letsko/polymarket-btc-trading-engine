#include <chrono>
#include <string>
#include <vector>
#include <ixwebsocket/IXHttpClient.h>
#include "simdjson.h"
#include <iostream>
#include "get_market.h"

int64_t current_boundary() {
    auto now = std::chrono::system_clock::now();
    int64_t now_secs = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    return (now_secs / 300) * 300;  // round down to nearest 5 minutes
}

// Gamma stores outcomes and clobTokenIds as JSON arrays encoded INSIDE a
// string field, e.g. "[\"Up\", \"Down\"]", so each needs a second parse.
// Returns owned strings: string_views from this parse would dangle as soon
// as the parser is reused for the next array.
static std::vector<std::string> parse_string_array(simdjson::ondemand::parser& parser,
                                                   const std::string& raw) {
    std::vector<std::string> result;
    simdjson::padded_string padded(raw);
    auto doc = parser.iterate(padded);
    for (auto item : doc.get_array()) {
        std::string_view value = item;
        result.emplace_back(value);
    }
    return result;
}

// static = private to this file. Takes the slug rather than deciding it,
// so callers can ask for a window other than the one running right now
// (needed to pre-fetch the upcoming market before its window starts).
//
// Never throws: every failure (network, HTTP status, market not listed
// yet, unexpected JSON shape) returns a default-constructed Market, which
// callers detect with valid() and retry. An exception escaping here would
// reach clob_ws's main thread uncaught, call std::terminate, and skip the
// Recorder's destructor - losing every record still queued.
//
// Errors go to std::cerr, never std::cout: stdout carries the recorded
// JSONL stream, and a stray line there corrupts the dataset.
static Market fetch_market(const std::string& slug) {

    Market market_result;

    ix::HttpClient httpClient;
    std::string url = "https://gamma-api.polymarket.com/events?slug=" + slug;
    auto args = httpClient.createRequest(url);

    // IXWebSocket defaults to a 60s connect and 1800s transfer timeout.
    // This call sits on the market-rotation path: the pre-subscribe fires
    // 30s before the boundary and retries every 3s, so a request that
    // hangs for longer than a few seconds is worth abandoning and retrying,
    // not waiting on through the boundary.
    args->connectTimeout = 3;
    args->transferTimeout = 5;

    ix::HttpResponsePtr response = httpClient.get(url, args);

    if (response->statusCode != 200) {
        // statusCode is 0 for network-level failures (DNS, timeout, TLS),
        // in which case errorMsg is the only thing that says what happened.
        std::cerr << "[get_market] request failed for " << slug
                  << ", status=" << response->statusCode
                  << " error=" << response->errorMsg << std::endl;
        return market_result;
    }

    try {
        simdjson::ondemand::parser parser;
        simdjson::padded_string padded = simdjson::padded_string(response->body);
        auto doc = parser.iterate(padded);

        // A slug with no market yet returns 200 with an empty array "[]",
        // not a 404. Checked explicitly so the log says what happened,
        // rather than surfacing as a generic INDEX_OUT_OF_BOUNDS.
        auto event = doc.get_array().at(0);
        if (event.error() == simdjson::INDEX_OUT_OF_BOUNDS) {
            std::cerr << "[get_market] no market listed yet for " << slug << std::endl;
            return market_result;
        }

        auto market = event["markets"].at(0);

        // Copied into owned strings before the second parse below: the
        // string_views simdjson hands out point into this parser's
        // internal buffer, so they are kept only as long as needed.
        std::string_view condition_id = market["conditionId"];
        std::string_view outcomes_raw = market["outcomes"];
        std::string outcomes_str(outcomes_raw);
        std::string_view clob_token_ids_raw = market["clobTokenIds"];
        std::string token_ids_str(clob_token_ids_raw);

        simdjson::ondemand::parser inner_parser;
        std::vector<std::string> outcomes = parse_string_array(inner_parser, outcomes_str);
        std::vector<std::string> token_ids = parse_string_array(inner_parser, token_ids_str);

        if (outcomes.size() != 2 || token_ids.size() != 2) {
            std::cerr << "[get_market] expected 2 outcomes/tokens for " << slug
                      << ", got " << outcomes.size() << "/" << token_ids.size() << std::endl;
            return market_result;
        }

        // Pair each token with its outcome by name rather than assuming
        // Up is listed first. The two arrays are index-aligned by Gamma;
        // the order within them is not something we control.
        Market parsed;
        parsed.condition_id = std::string(condition_id);
        for (size_t i = 0; i < 2; ++i) {
            if (outcomes[i] == "Up") {
                parsed.token_up = token_ids[i];
            } else if (outcomes[i] == "Down") {
                parsed.token_down = token_ids[i];
            }
        }

        if (!parsed.valid()) {
            std::cerr << "[get_market] unexpected outcomes for " << slug
                      << ": " << outcomes_str << std::endl;
            return market_result;
        }

        return parsed;

    } catch (const simdjson::simdjson_error& e) {
        // Any field missing or the wrong type - Gamma changed its schema,
        // or returned an error body with a 200.
        std::cerr << "[get_market] failed to parse response for " << slug
                  << ": " << e.what() << std::endl;
        std::cerr << "  raw: " << response->body.substr(0, 300) << std::endl;
        return market_result;
    }
}

Market get_market_for_boundary(int64_t boundary_ts) {
    return fetch_market("btc-updown-5m-" + std::to_string(boundary_ts));
}

Market get_current_market() {
    return get_market_for_boundary(current_boundary());
}
