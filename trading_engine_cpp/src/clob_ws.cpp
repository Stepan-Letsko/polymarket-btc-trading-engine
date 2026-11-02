#include <iostream>
#include <ixwebsocket/IXWebSocket.h>
#include <thread>
#include <chrono>
#include "get_market.h"
#include "simdjson.h"
#include <mutex>
#include <atomic>
#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include "reconnector.h"
#include "recorder.h"
#include "runtime.h" // g_shutdown, install_signal_handlers, now_millis, sleep_until_ms

// Shared between the main thread and the websocket callback thread.
// Everything in here changes together at a market rotation, so one
// mutex guards all of it.
struct SharedState {
    std::mutex mutex;
    std::string current_up;
    std::string current_down;
    std::string current_market;        // condition id, to filter market_resolved
    int64_t current_window = 0;
    std::string current_subscription;

    // Set by the callback thread when a (re)connection opens, read and
    // cleared by the main loop. A reconnect wipes the server's record of
    // our subscriptions, so any pre-subscribe we'd already done for the
    // upcoming window is gone and has to be redone. atomic rather than
    // mutex-guarded because it's a single flag, not a group of fields
    // that must change together.
    std::atomic<bool> reconnected{false};
};

std::string build_subscription(const std::string& up, const std::string& down) {
    return "{\"assets_ids\":[\"" + up + "\",\"" + down + "\"],\"type\":\"market\",\"initial_dump\":true,\"custom_feature_enabled\":true}";
} //Function that takes in up and down market and builds an API subscribe event

std::string build_unsubscribe(const std::string& up, const std::string& down) {
    return "{\"operation\":\"unsubscribe\",\"assets_ids\":[\"" + up + "\",\"" + down + "\"]}";
} //Function that takes in up and down market and builds an API unsubscribe event

std::string build_add_subscription(const std::string& up, const std::string& down) {
    return "{\"operation\":\"subscribe\",\"assets_ids\":[\"" + up + "\",\"" + down + "\"],\"custom_feature_enabled\":true}";
}

// Handles one event. Takes a simdjson value rather than a document so
// the same code works for a lone event object and for each element of a
// batched array - doc.get_value() converts a document into one.
//
// Note the early `return`s below now skip just this event, not the whole
// message, which is what we want when several events arrive batched.
void handle_event(simdjson::ondemand::value event,
                  const std::string& cur_up,
                  const std::string& cur_down,
                  const std::string& cur_market) {

    std::string_view event_type = event["event_type"];

    if (event_type == "book") {
        std::string_view asset_id = event["asset_id"];
        if (asset_id != cur_up && asset_id != cur_down) return;
        const char* token = (asset_id == cur_up) ? "UP" : "DOWN";
        std::string_view timestamp = event["timestamp"];
        std::cerr << "[book] " << token << " ts=" << timestamp << " bids: ";
        for (auto level : event["bids"]) {
            std::string_view price = level["price"];
            std::string_view size = level["size"];
            std::cerr << price << "@" << size << " ";
        }
        std::cerr << "| asks: ";
        for (auto level : event["asks"]) {
            std::string_view price = level["price"];
            std::string_view size = level["size"];
            std::cerr << price << "@" << size << " ";
        }
        std::cerr << "\n";

    } else if (event_type == "price_change") {
        std::string_view timestamp = event["timestamp"];

        // Build the line first and only print if something survived the
        // filter - a single event can carry changes for the upcoming
        // market only, and we don't want an empty row in the output.
        std::string line;
        for (auto change : event["price_changes"]) {
            // asset_id lives inside each entry here, not at event level,
            // so the filter goes in the loop.
            std::string_view asset_id = change["asset_id"];
            if (asset_id != cur_up && asset_id != cur_down) continue;
            const char* token = (asset_id == cur_up) ? "UP" : "DOWN";
            std::string_view price = change["price"];
            std::string_view size = change["size"];
            std::string_view side = change["side"];
            line += std::string(token) + " " + std::string(side) + " "
                  + std::string(price) + "@" + std::string(size) + " | ";
        }
        if (!line.empty()) {
            std::cerr << "[price_change] ts=" << timestamp << " " << line << "\n";
        }

    } else if (event_type == "last_trade_price") {
        std::string_view asset_id = event["asset_id"];
        if (asset_id != cur_up && asset_id != cur_down) return;
        const char* token = (asset_id == cur_up) ? "UP" : "DOWN";
        std::string_view price = event["price"];
        std::string_view size = event["size"];
        std::string_view side = event["side"];
        std::cerr << "[last_trade_price] " << token << " " << side << " "
                  << price << "@" << size << "\n";

    } else if (event_type == "tick_size_change") {
        std::string_view asset_id = event["asset_id"];
        if (asset_id != cur_up && asset_id != cur_down) return;
        const char* token = (asset_id == cur_up) ? "UP" : "DOWN";
        std::string_view old_tick = event["old_tick_size"];
        std::string_view new_tick = event["new_tick_size"];
        std::cerr << "[tick_size_change] " << token << " " << old_tick
                  << " -> " << new_tick << "\n";

    } else if (event_type == "best_bid_ask") {
        std::string_view asset_id = event["asset_id"];
        if (asset_id != cur_up && asset_id != cur_down) return;
        const char* token = (asset_id == cur_up) ? "UP" : "DOWN";
        std::string_view best_bid = event["best_bid"];
        std::string_view best_ask = event["best_ask"];
        std::string_view spread = event["spread"];
        std::cerr << "[best_bid_ask] " << token << " bid=" << best_bid
                  << " ask=" << best_ask << " spread=" << spread << "\n";

    } else if (event_type == "new_market") {
        std::string_view slug = event["slug"];
        if (slug.rfind("btc-updown-5m-", 0) == 0) {
            std::cerr << "[new_market] BTC window: " << slug << std::endl;
        }

    } else if (event_type == "market_resolved") {
        // Broadcast globally like new_market, but with no slug to filter
        // on - so match the condition id against the market we're on.
        std::string_view resolved_market = event["market"];
        if (resolved_market != cur_market) return;
        std::string_view winning_outcome = event["winning_outcome"];
        std::cerr << "[market_resolved] outcome=" << winning_outcome << std::endl;

    } else {
        std::cerr << "[unknown event_type] " << event_type << "\n";
    }
}

static const char* kDefaultUrl = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

static void print_usage(const char* program) {
    std::cerr << "Usage: " << program << " [--verbose] [--url <ws-url>]\n"
              << "  --verbose   print every parsed event to stderr (slow:\n"
              << "              each print blocks the receiving thread)\n"
              << "  --url       connect somewhere other than " << kDefaultUrl << "\n"
              << "              (for testing reconnects against a local server)\n";
}

// argc/argv: the words typed on the command line - see binance_ws.cpp.
int main(int argc, char* argv[]) {

    // Off by default: printing every event means a blocking write to the
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
            // "./clob_ws --url" alone is an error, not a crash.
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

    ix::WebSocket webSocket; //Creating the websocket
    webSocket.setUrl(url); //Websocket URL

    simdjson::ondemand::parser parser; //Creating the simdjson parser

    SharedState state; //Creating the shared state structure

    // Starts the writer thread. Destination is std::cout deliberately:
    // the Recorder does not decide where records land, so file naming,
    // rotation and compression are a launch-time choice, e.g.
    //     ./clob_ws > Data/clob.jsonl
    //     ./clob_ws | gzip > Data/clob.jsonl.gz
    // Declared before setOnMessageCallback below, which captures it.
    Recorder recorder(std::cout);

    // Keep retrying rather than starting up with empty tokens - an empty
    // subscription would connect successfully and then sit silent
    // forever, which is far harder to notice than a startup failure.
    Market market = get_current_market();
    while (!market.valid()) {
        std::cerr << "Could not fetch current market, retrying in 3s..." << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(3));
        market = get_current_market();
    }

    {
        std::lock_guard<std::mutex> lock(state.mutex);
        state.current_up = market.token_up;
        state.current_down = market.token_down;
        state.current_market = market.condition_id;
        state.current_window = current_boundary();
        state.current_subscription = build_subscription(market.token_up, market.token_down);
    }


    // Messages recorded since the last [stats] line. The callback adds 1,
    // the main thread reads and resets it - two threads, so atomic.
    std::atomic<uint64_t> msg_count{0};

    // Reconnect decisions and the silence watchdog (see reconnector.h).
    // The callback reports to it; the main loop calls poll().
    //
    // 30s silence limit, longer than Binance/PolyBolt's 10s: CLOB traffic
    // depends on trading activity rather than a fixed rate, so it can
    // legitimately pause for a few seconds. The CLOB has no documented
    // close codes, so every close is retried with backoff - the default.
    Reconnector reconnector(webSocket, ReconnectConfig{"clob", 30000});

    // verbose by VALUE (set once, never changes, so a copy is safe);
    // msg_count and reconnector by REFERENCE (both threads must share the
    // one object).
    webSocket.setOnMessageCallback([&webSocket, &state, &parser, &recorder, &msg_count, &reconnector, verbose](const ix::WebSocketMessagePtr & msg) { //Setting the callback function that will be called when a message is received from the websocket server
        if (msg->type == ix::WebSocketMessageType::Open) { //If the message type is Open, it means the connection has been established
            std::string sub;
            {
                std::lock_guard<std::mutex> lock(state.mutex);
                sub = state.current_subscription;
            }
            std::cerr << "Connection opened, sending subscription..." << std::endl; // Printing a message to the console
            webSocket.send(sub); // Sending the subscription message to the websocket server

            // A reconnect wipes the server's view of our subscriptions.
            // Any pre-subscribe already done for the upcoming window is
            // gone, so tell the main loop to redo it - otherwise we'd
            // rotate to a market we were never actually subscribed to on
            // this connection and go silent for a whole window.
            state.reconnected.store(true);
        } else if (msg->type == ix::WebSocketMessageType::Message) { //If the message type is Message, it means a message has been received from the websocket server
            
            int64_t recv_ts = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();

            if (msg->str == "PONG") {
                return;
            }

            // Fed AFTER the PONG check on purpose: PONG only proves the
            // server answers our pings, not that market data is flowing - a
            // connection that pongs but sends no data is exactly what the
            // watchdog must catch. Fed BEFORE the market filter below, so
            // the upcoming market's warm-up traffic (received but not
            // recorded) still counts as the feed being alive.
            reconnector.on_message();

            std::string cur_up, cur_down, cur_market;
            int64_t cur_window;
            {
                std::lock_guard<std::mutex> lock(state.mutex);
                cur_up = state.current_up;
                cur_down = state.current_down;
                cur_market = state.current_market;
                cur_window = state.current_window;
            }

            // Drop anything that isn't the live market, before it's
            // recorded or printed.
            //
            // We subscribe to the next window 30s early purely so the
            // connection is warm at the boundary - we don't want its
            // data until it actually goes live. Without this, those
            // early messages would be recorded carrying the CURRENT
            // window's label, which would be actively wrong.
            //
            // A substring check on the raw text rather than a parse:
            // every event for a market carries its condition id, and
            // this keeps capture independent of the parser.
            if (msg->str.find(cur_market) == std::string::npos) {
                return;
            }

            // Counted after the filter, so [stats] reports what was actually
            // recorded - not the upcoming market's warm-up traffic we drop.
            msg_count++;

            // The recorded record: our receive time, which market window
            // it belongs to, and the original message spliced in
            // untouched. Written BEFORE parsing deliberately - capture
            // shouldn't depend on our parser succeeding, so a new or
            // unexpected event type costs a log line, not the data.
            //
            // Handed to the Recorder as one finished string rather than
            // streamed piece by piece with <<, because this thread must
            // not touch I/O - write() only queues it. The writer thread
            // does the actual output.
            //
            // The trailing newline has to be stripped first: the
            // initial_dump message arrives with one, and splicing it in
            // raw would break the record across two lines - one line
            // missing its closing brace, the next a lone "}". That
            // corrupts the JSONL for every connection and every
            // reconnect after it.
            std::string_view payload(msg->str);
            while (!payload.empty() && (payload.back() == '\n' || payload.back() == '\r')) {
                payload.remove_suffix(1);
            }

            std::string record = "{\"recv_ts\":" + std::to_string(recv_ts) +
                                 ",\"window\":" + std::to_string(cur_window) +
                                 ",\"data\":";
            record.append(payload);
            record += "}\n";
            recorder.write(std::move(record));

            // Recording is done. Parsing below only feeds handle_event's
            // prints, so without --verbose skip it entirely.
            if (!verbose) {
                return;
            }

            try {
                simdjson::padded_string padded = simdjson::padded_string(msg->str);
                auto doc = parser.iterate(padded);

                // Polymarket sends either a single event object, or an
                // ARRAY of events batched together (the initial_dump
                // delivers both book snapshots this way). clob_ws.py
                // handles the same thing with
                //     events = data if isinstance(data, list) else [data]
                if (doc.type() == simdjson::ondemand::json_type::array) {
                    for (auto item : doc.get_array()) {
                        handle_event(item.value(), cur_up, cur_down, cur_market);
                    }
                } else {
                    handle_event(doc.get_value().value(), cur_up, cur_down, cur_market);
                }

            } catch (const simdjson::simdjson_error& e) {
                std::cerr << "Parse error: " << e.what() << std::endl;
                std::cerr << "  raw: " << msg->str.substr(0, 300) << std::endl;
            }

        } else if (msg->type == ix::WebSocketMessageType::Close) {
            std::cerr << "Connection closed: code=" << msg->closeInfo.code
                      << " reason=" << msg->closeInfo.reason << std::endl;
            reconnector.on_disconnect(msg->closeInfo.code);

        } else if (msg->type == ix::WebSocketMessageType::Error) { //If the message type is Error, it means there was an error with the websocket connection
            // With automatic reconnection off (the Reconnector turns it
            // off), an Error means a connect attempt failed - a disconnect
            // too, with no close code.
            std::cerr << "Error: " << msg->errorInfo.reason
                      << " (http status " << msg->errorInfo.http_status << ")" << std::endl;
            reconnector.on_disconnect(0);
        }
    });

    webSocket.setPingMessage("PING", ix::SendMessageKind::Text);
    webSocket.setPingInterval(9); //Setting the ping interval to 9 seconds (this is to keep the connection alive)

    // Ctrl+C / SIGTERM now flip g_shutdown instead of killing the process,
    // so the main loop below exits and the Recorder drains. See runtime.h.
    // Installed BEFORE connecting so there is no moment where messages are
    // being queued but a Ctrl+C would still kill us outright.
    install_signal_handlers();

    // Starts the first connection (and the watchdog's clock).
    reconnector.start();

    int64_t current_boundary_ts = current_boundary(); //The 5m window we currently treat as live
    int64_t prefetched_for = 0;  //Which upcoming boundary we've already subscribed to
    Market pending;              //That upcoming window's tokens

    // [stats] every 5s, same as binance_ws: a silent terminal still shows
    // the feed is alive, and "0 msgs" shows when it has quietly died.
    const int64_t stats_interval_ms = 5000;
    int64_t last_stats_ms = now_millis();
    int64_t next_stats_ms = last_stats_ms + stats_interval_ms;
    uint64_t last_dropped = 0;
    int exit_code = 0;

    // Rather than polling on a fixed interval (which left up to a full
    // second of lag at the boundary - the single most important moment
    // in the window), sleep exactly until the next thing that needs
    // doing: either the pre-subscribe at boundary-30s, or the boundary
    // itself. The loop wakes up on the event, not on a timer.
    while (!g_shutdown) {
        int64_t next_boundary = current_boundary_ts + 300;        //The window starting after this one
        int64_t prefetch_at_ms = (next_boundary - 30) * 1000;     //When to pre-subscribe
        int64_t boundary_at_ms = next_boundary * 1000;            //When the new window opens

        //Whichever of those two is still ahead of us is the next rotation step.
        int64_t target_ms = (prefetched_for != next_boundary) ? prefetch_at_ms : boundary_at_ms;

        //Sleep until the rotation step, the next [stats] line, or 200ms
        //from now - whichever comes first. The 200ms cap is so the
        //Reconnector is polled often enough to act on a disconnect or a
        //silent feed within 0.2s. Waking early is harmless: both rotation
        //blocks below check the clock themselves, so if neither is due
        //yet they do nothing and we fall through.
        //
        //Interrupted by Ctrl+C - leave now rather than running a rotation
        //on the way out.
        if (!sleep_until_ms(std::min({target_ms, next_stats_ms, now_millis() + 200}))) {
            break;
        }

        int64_t now_ms = now_millis();

        // A reconnect happened since we last looked, so the server no
        // longer knows about any pre-subscribe we did. Forget it, and the
        // logic below will redo it on the new connection.
        if (state.reconnected.exchange(false)) {
            prefetched_for = 0;
        }

        // --- Pre-subscribe, 30s before the next window opens ---
        // Subscribing early means the connection is already warm when the
        // boundary hits, so there's no HTTP fetch sitting on the critical
        // path. state.current_* is deliberately NOT touched here, so the
        // asset_id filter keeps discarding this market's data until it
        // actually goes live.
        if (prefetched_for != next_boundary && now_ms >= prefetch_at_ms && now_ms < boundary_at_ms) {
            std::cerr << "--- pre-subscribing to upcoming window " << next_boundary << " ---" << std::endl;
            Market fetched = get_market_for_boundary(next_boundary);
            if (!fetched.valid()) {
                //Don't record it as prefetched - we'll retry on the next
                //pass. Back off first so a persistent failure doesn't spin
                //hammering the API.
                std::cerr << "--- prefetch failed, retrying in 3s ---" << std::endl;
                sleep_until_ms(now_millis() + 3000);
                continue;
            }
            pending = fetched;
            prefetched_for = next_boundary;
            webSocket.send(build_add_subscription(pending.token_up, pending.token_down));
            continue; //Recompute - the next thing to wait for is the boundary itself
        }

        // --- Boundary reached: promote the pending market to live ---
        if (now_ms >= boundary_at_ms) {
            Market promoted;
            if (prefetched_for == next_boundary && pending.valid()) {
                promoted = pending; //Already subscribed during the pre-fetch, nothing to send
            } else {
                //No prefetch available (just started up inside the last 30s,
                //the prefetch failed, or a reconnect wiped it) - fetch and
                //subscribe now.
                std::cerr << "--- no prefetch available, fetching now ---" << std::endl;
                promoted = get_market_for_boundary(next_boundary);
                if (!promoted.valid()) {
                    //Keep the old market live rather than replacing it with
                    //empty tokens, and retry without advancing the boundary.
                    std::cerr << "--- fetch failed for window " << next_boundary
                              << ", retrying in 3s ---" << std::endl;
                    sleep_until_ms(now_millis() + 3000);
                    continue;
                }
                webSocket.send(build_add_subscription(promoted.token_up, promoted.token_down));
            }

            current_boundary_ts = next_boundary;

            std::string old_up, old_down;
            {
                std::lock_guard<std::mutex> lock(state.mutex);
                old_up = state.current_up;
                old_down = state.current_down;
                state.current_up = promoted.token_up;
                state.current_down = promoted.token_down;
                state.current_market = promoted.condition_id;
                state.current_window = next_boundary;
                state.current_subscription = build_subscription(promoted.token_up, promoted.token_down);
            }

            std::cerr << "--- now live on window " << next_boundary << " ---" << std::endl;
            webSocket.send(build_unsubscribe(old_up, old_down));
        }

        // --- Reconnect decisions and the silence watchdog ---
        // AFTER the rotation, for the same reason as the stats line below:
        // at a boundary the market switch runs first. The rotation's 3s
        // retry paths `continue` past this, which can delay a reconnect by
        // up to those few seconds - acceptable, and only while Gamma is
        // already failing.
        //
        // false = a fatal close. The CLOB's rule retries every close, so
        // this can't happen today - but the check keeps it correct if the
        // rule ever changes.
        if (!reconnector.poll()) {
            exit_code = 1;
            break;
        }

        // --- [stats] line ---
        // Deliberately LAST in the loop: if a boundary and a stats line fall
        // due together, the rotation runs first, so a slow terminal write
        // can never delay the market switch.
        //
        // The rotation's retry paths above sleep 3s and `continue`, which
        // can push a stats line late. So the line reports the real elapsed
        // time rather than assuming exactly 5s, and next_stats_ms skips
        // ahead past any slots that were missed instead of firing a burst
        // of catch-up lines.
        int64_t stats_now_ms = now_millis();
        if (stats_now_ms >= next_stats_ms) {
            // exchange(0) reads and resets in one step, so no message
            // counted between a separate read and reset can be lost.
            uint64_t recorded = msg_count.exchange(0);
            uint64_t dropped_total = recorder.dropped();
            uint64_t dropped_now = dropped_total - last_dropped;
            last_dropped = dropped_total;

            std::cerr << "[stats] " << recorded << " msgs in last "
                      << (stats_now_ms - last_stats_ms) / 1000.0 << "s, "
                      << dropped_now << " dropped" << std::endl;

            last_stats_ms = stats_now_ms;
            while (next_stats_ms <= stats_now_ms) {
                next_stats_ms += stats_interval_ms;
            }
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