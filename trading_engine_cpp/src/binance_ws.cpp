#include <iostream>
#include <ixwebsocket/IXWebSocket.h>
#include <thread>
#include <chrono>
#include <string_view>
#include "simdjson.h"
#include "recorder.h"
#include "runtime.h" // g_shutdown, install_signal_handlers


int main (){
    ix::WebSocket webSocket;

    webSocket.setUrl("wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker/btcusdt@depth5@100ms");

    simdjson::ondemand::parser parser;

    // Starts the writer thread. Destination is std::cout deliberately, same
    // as clob_ws: the Recorder does not decide where records land, so file
    // naming and compression are a launch-time choice, e.g.
    //     ./binance_ws > Data/binance.jsonl
    //     ./binance_ws | gzip > Data/binance.jsonl.gz
    // That makes stdout the data channel - every log line below goes to
    // std::cerr so it can never end up inside the recorded file.
    // Declared before setOnMessageCallback below, which captures it.
    Recorder recorder(std::cout);

    webSocket.setOnMessageCallback([&parser, &recorder](const ix::WebSocketMessagePtr & msg) {
        if (msg->type == ix::WebSocketMessageType::Message) {
            // Taken first, before any other work in the callback, so it is
            // as close as possible to when the message actually arrived.
            // Microseconds, matching clob_ws, so the two feeds can be
            // merged on recv_ts later.
            int64_t recv_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

            // Strip any trailing newline before splicing the message in -
            // one inside the payload would split the record across two
            // lines and corrupt the JSONL.
            std::string_view payload(msg->str);
            while (!payload.empty() && (payload.back() == '\n' || payload.back() == '\r')) {
                payload.remove_suffix(1);
            }

            // The recorded record: our receive time, and the original
            // message spliced in untouched. Written BEFORE parsing
            // deliberately - capture shouldn't depend on our parser
            // succeeding. Built as one finished string because write()
            // only queues it; the writer thread does the actual output.
            //
            // No "window" field, unlike clob_ws: a Binance message isn't
            // tied to a Polymarket market, and its window is just
            // recv_ts rounded down to 5 minutes if it's ever needed.
            std::string record = "{\"recv_ts\":" + std::to_string(recv_ts) +
                                 ",\"data\":";
            record.append(payload);
            record += "}\n";
            recorder.write(std::move(record));

            // Parsing only feeds the debug prints below; the record above is
            // already queued. So a message we fail to understand (a Binance
            // error payload, a changed field, a truncated frame) costs a log
            // line, not the data - and the exception never escapes into
            // IXWebSocket's thread, where nothing would catch it and the whole
            // process would terminate, losing everything still queued.
            try {
                simdjson::padded_string padded = simdjson::padded_string(msg->str);
                auto doc = parser.iterate(padded);

                std::string_view stream = doc["stream"];
                auto data = doc["data"];

                if (stream == "btcusdt@depth5@100ms") {
                    int64_t lastUpdateId = data["lastUpdateId"];
                    std::cerr << "[depth5] lastUpdateId=" << lastUpdateId << " bids: ";
                    for (auto bid_level : data["bids"]) {
                        for (auto price_or_qty : bid_level) {
                            std::string_view v = price_or_qty;
                            std::cerr << v << " ";
                        }
                        std::cerr << "| ";
                    }
                    std::cerr << std::endl;

                } else if (stream == "btcusdt@bookTicker") {
                    std::string_view symbol = data["s"];
                    std::string_view best_bid = data["b"];
                    std::string_view best_bid_qty = data["B"];
                    std::string_view best_ask = data["a"];
                    std::string_view best_ask_qty = data["A"];

                    std::cerr << "[bookTicker] " << symbol
                            << " bid=" << best_bid << "(" << best_bid_qty << ")"
                            << " ask=" << best_ask << "(" << best_ask_qty << ")" << std::endl;

                } else if (stream == "btcusdt@aggTrade") {
                    int64_t event_time = data["E"];
                    std::string_view symbol = data["s"];
                    std::string_view price = data["p"];
                    std::string_view qty = data["q"];
                    bool buyer_is_maker = data["m"];

                    std::cerr << "[aggTrade] " << symbol
                            << " price=" << price
                            << " qty=" << qty
                            << " E=" << event_time
                            << " buyer_is_maker=" << buyer_is_maker << std::endl;
                }

            } catch (const simdjson::simdjson_error& e) {
                std::cerr << "Parse error: " << e.what() << std::endl;
                std::cerr << "  raw: " << msg->str.substr(0, 300) << std::endl;
            }

        } else if (msg->type == ix::WebSocketMessageType::Open) {
            std::cerr << "Connection opened" << std::endl;
        } else if (msg->type == ix::WebSocketMessageType::Error) {
            std::cerr << "Error: " << msg->errorInfo.reason << std::endl;
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

    // Ctrl+C / SIGTERM now flip g_shutdown instead of killing the process.
    // Installed BEFORE start() so there is no moment where messages are
    // being queued but a Ctrl+C would still kill us outright.
    install_signal_handlers();

    webSocket.start();

    // All the real work happens on IXWebSocket's thread (the callback) and
    // the Recorder's writer thread. The main thread only has to stay alive
    // until asked to stop - so it naps in short 200ms chunks, checking the
    // flag each time, so Ctrl+C is noticed almost instantly.
    while (!g_shutdown) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // Stop the websocket FIRST, so no callback can fire while we tear
    // down. Then returning from main destroys the locals, and the
    // Recorder's destructor drains the queue and joins the writer thread
    // - the process does not exit until the last record is written.
    std::cerr << "--- shutting down ---" << std::endl;
    webSocket.stop();

    if (recorder.dropped() > 0) {
        std::cerr << "--- WARNING: dropped " << recorder.dropped()
                  << " records ---" << std::endl;
    }

    return 0;
}
