#include <iostream>
#include <ixwebsocket/IXWebSocket.h>
#include <thread>
#include <chrono>
#include "simdjson.h"


int main (){
    ix::WebSocket webSocket;

    webSocket.setUrl("wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker/btcusdt@depth5@100ms");

    simdjson::ondemand::parser parser;

    webSocket.setOnMessageCallback([&parser](const ix::WebSocketMessagePtr & msg) {
        if (msg->type == ix::WebSocketMessageType::Message) {
            simdjson::padded_string padded = simdjson::padded_string(msg->str);
            auto doc = parser.iterate(padded);

            std::string_view stream = doc["stream"];
            auto data = doc["data"];

            if (stream == "btcusdt@depth5@100ms") {
                int64_t lastUpdateId = data["lastUpdateId"];
                std::cout << "[depth5] lastUpdateId=" << lastUpdateId << " bids: ";
                for (auto bid_level : data["bids"]) {
                    for (auto price_or_qty : bid_level) {
                        std::string_view v = price_or_qty;
                        std::cout << v << " ";
                    }
                    std::cout << "| ";
                }
                std::cout << std::endl;

            } else if (stream == "btcusdt@bookTicker") {
                std::string_view symbol = data["s"];
                std::string_view best_bid = data["b"];
                std::string_view best_bid_qty = data["B"];
                std::string_view best_ask = data["a"];
                std::string_view best_ask_qty = data["A"];

                std::cout << "[bookTicker] " << symbol
                        << " bid=" << best_bid << "(" << best_bid_qty << ")"
                        << " ask=" << best_ask << "(" << best_ask_qty << ")" << std::endl;

            } else if (stream == "btcusdt@aggTrade") {
                int64_t event_time = data["E"];
                std::string_view symbol = data["s"];
                std::string_view price = data["p"];
                std::string_view qty = data["q"];
                bool buyer_is_maker = data["m"];

                std::cout << "[aggTrade] " << symbol
                        << " price=" << price
                        << " qty=" << qty
                        << " E=" << event_time
                        << " buyer_is_maker=" << buyer_is_maker << std::endl;
            }

        } else if (msg->type == ix::WebSocketMessageType::Open) {
            std::cout << "Connection opened" << std::endl;
        } else if (msg->type == ix::WebSocketMessageType::Error) {
            std::cout << "Error: " << msg->errorInfo.reason << std::endl;
        }
    });

    // IXWebSocket already auto-reconnects on a hard drop/error by
    // default (_automaticReconnection is true out of the box). But it
    // does NOT actively check the connection is still alive otherwise
    // (default ping interval is -1, i.e. off) — without this, a
    // connection that silently goes stale (still "open", but no longer
    // delivering data, same failure mode we hit in the Python recorder)
    // would never trigger a reconnect on its own. Sending our own PING
    // every 30s gives it something to notice when the connection has
    // actually died.
    webSocket.setPingInterval(30);

    webSocket.start();

    while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    }

return 0;
}
