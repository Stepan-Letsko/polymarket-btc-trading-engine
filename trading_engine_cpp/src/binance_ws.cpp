#include <iostream>
#include <ixwebsocket/IXWebSocket.h>
#include <thread>
#include <chrono>
#include <string>
#include <string_view>
#include <atomic>
#include <cstdint>
#include <algorithm>
#include "simdjson.h"
#include "reconnector.h"
#include "recorder.h"
#include "runtime.h" // g_shutdown, install_signal_handlers, now_millis, sleep_until_ms


// timeUnit=MICROSECOND makes Binance send aggTrade's E and T in
// microseconds (16 digits) instead of the default milliseconds (13
// digits) - the same unit as our recv_ts. Milliseconds round away up to
// 999us, which hides the ordering of trades within the same ms and adds
// up to 1ms of error to every recv_ts - T latency measurement.
// bookTicker and depth5 carry no timestamps, so they are unaffected.
static const char* kDefaultUrl =
    "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker/btcusdt@depth5@100ms&timeUnit=MICROSECOND";

static void print_usage(const char* program) {
    std::cerr << "Usage: " << program << " [--verbose] [--url <ws-url>]\n"
              << "  --verbose   print every parsed message to stderr (slow:\n"
              << "              each print blocks the receiving thread)\n"
              << "  --url       connect somewhere other than Binance\n"
              << "              (for testing reconnects against a local server)\n";
}

// argc = how many words were typed, including the program name itself.
// argv = those words: argv[0] is the program, argv[1] onwards the options.
//   ./binance_ws --verbose   ->   argc = 2, argv[1] = "--verbose"
int main (int argc, char* argv[]){

    // Off by default: printing every message means a blocking write to the
    // terminal on the same thread that stamps recv_ts, delaying the
    // timestamps of every message queued behind it.
    bool verbose = false;
    std::string url = kDefaultUrl;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if (arg == "--verbose") {
            verbose = true;
        } else if (arg == "--url") {
            // The next word is the value. i + 1 < argc checks it exists, so
            // "./binance_ws --url" alone is an error, not a crash.
            if (i + 1 >= argc) {
                std::cerr << "--url needs a value\n";
                print_usage(argv[0]);
                return 1;
            }
            url = argv[++i];
        } else if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        } else {
            // Refuse anything unrecognised rather than ignoring it - a typo
            // like --verbos would otherwise run silently and look broken.
            std::cerr << "Unknown option: " << arg << "\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    ix::WebSocket webSocket;
    webSocket.setUrl(url);

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

    // Messages received since the last [stats] line. Two threads touch it:
    // the callback adds 1 per message, the main thread reads and resets it
    // every 5s. A plain uint64_t touched by two threads at once is a data
    // race (the result can be garbage), so it's atomic - same as the
    // Recorder's dropped_ counter.
    std::atomic<uint64_t> msg_count{0};

    // Reconnect decisions and the silence watchdog (see reconnector.h).
    // The callback reports to it; the main loop calls poll().
    //
    // 10s silence limit: depth5 alone sends 10 messages a second and the
    // whole stream normally runs at 100+/s, so 10s of nothing is clearly a
    // dead feed, not a quiet market. Binance has no special close codes
    // (it also disconnects everyone every 24h with a normal close), so
    // every close is simply retried with backoff - the default rule.
    Reconnector reconnector(webSocket, ReconnectConfig{"binance", 10000});

    // verbose is captured by VALUE (no &): the callback runs on IXWebSocket's
    // thread and gets its own copy. Safe because verbose is set once above
    // and never changes, so the two threads can never disagree about it.
    // msg_count and reconnector are captured by REFERENCE (&): both threads
    // must share the one object, not each have a copy.
    webSocket.setOnMessageCallback([&parser, &recorder, &msg_count, &reconnector, verbose](const ix::WebSocketMessagePtr & msg) {
        if (msg->type == ix::WebSocketMessageType::Message) {
            // Taken first, before any other work in the callback, so it is
            // as close as possible to when the message actually arrived.
            // Microseconds, matching clob_ws, so the two feeds can be
            // merged on recv_ts later.
            int64_t recv_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

            msg_count++;
            reconnector.on_message();  // feeds the silence watchdog

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

            // Recording is done. Everything below only exists to print the
            // message, so without --verbose skip it entirely - no parse, no
            // print, and the thread is straight back to receiving.
            if (!verbose) {
                return;
            }

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

        } else if (msg->type == ix::WebSocketMessageType::Close) {
            std::cerr << "Connection closed: code=" << msg->closeInfo.code
                      << " reason=" << msg->closeInfo.reason << std::endl;
            reconnector.on_disconnect(msg->closeInfo.code);

        } else if (msg->type == ix::WebSocketMessageType::Error) {
            // With automatic reconnection off (the Reconnector turns it
            // off), an Error means a connect attempt failed - a disconnect
            // too, with no close code. http_status is set when Binance
            // refused the handshake, e.g. 429 (rate limited) or 418 (IP
            // temporarily banned for ignoring 429s).
            std::cerr << "Error: " << msg->errorInfo.reason
                      << " (http status " << msg->errorInfo.http_status << ")" << std::endl;
            reconnector.on_disconnect(0);
        }
    });

    // Our own PING every 30s. Without it, a connection whose network path
    // has died (no close, no error) is never noticed at the TCP level -
    // IXWebSocket closes the connection when a ping goes unanswered, and
    // that close reaches the Reconnector like any other. The silence
    // watchdog covers the other case: connection fine, data stopped.
    webSocket.setPingInterval(30);

    // Ctrl+C / SIGTERM now flip g_shutdown instead of killing the process.
    // Installed BEFORE start() so there is no moment where messages are
    // being queued but a Ctrl+C would still kill us outright.
    install_signal_handlers();

    reconnector.start();

    // All the real work happens on IXWebSocket's thread (the callback) and
    // the Recorder's writer thread. The main thread does two jobs: every
    // reconnect decision (through reconnector.poll()), and the one bit of
    // printing that runs by default - a [stats] line every 5s, so a silent
    // terminal still shows the feed is alive. Printing here rather than in
    // the callback keeps the receiving thread free of I/O.
    //
    // It wakes at least every 200ms, so a disconnect or a silent feed is
    // acted on within 0.2s. sleep_until_ms returns false on Ctrl+C, so
    // shutdown stays instant. next_stats_ms advances by a fixed 5000 rather
    // than "now + 5000", so the time spent printing doesn't make the
    // interval slowly drift.
    const int64_t stats_interval_ms = 5000;
    int64_t next_stats_ms = now_millis() + stats_interval_ms;
    uint64_t last_dropped = 0;
    int exit_code = 0;

    while (sleep_until_ms(std::min(next_stats_ms, now_millis() + 200))) {
        // false = a fatal close. Binance's rule retries every close, so
        // this can't happen today - but the check keeps it correct if the
        // rule ever changes.
        if (!reconnector.poll()) {
            exit_code = 1;
            break;
        }

        if (now_millis() >= next_stats_ms) {
            // exchange(0) reads the count and resets it in one step.
            // Reading, then setting to 0 separately, would lose any message
            // the callback counted in between the two.
            uint64_t received = msg_count.exchange(0);

            // dropped() is a running total, so report only what's new
            // since the last line.
            uint64_t dropped_total = recorder.dropped();
            uint64_t dropped_now = dropped_total - last_dropped;
            last_dropped = dropped_total;

            std::cerr << "[stats] " << received << " msgs in last "
                      << stats_interval_ms / 1000 << "s, "
                      << dropped_now << " dropped" << std::endl;

            next_stats_ms += stats_interval_ms;
        }
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

    return exit_code;
}
