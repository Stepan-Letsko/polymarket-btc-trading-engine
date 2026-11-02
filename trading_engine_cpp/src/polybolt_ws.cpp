// PolyBolt recorder - BTC TWAP (the resolution feed) and Chainlink spot.
//
// On every connection open: send auth, wait for the server's "authed"
// reply, then subscribe to both channels. Every message received - acks,
// snapshots and live prices alike - is recorded raw to stdout. See
// docs/polybolt_subscription.md for the connection sequence and
// docs/polybolt_message_types.md for what each message means.
//
// Credentials come from the environment, never from code:
//     set -a; source .env; set +a
//     ./build/polybolt_ws > Data/polybolt.jsonl

#include <iostream>
#include <ixwebsocket/IXWebSocket.h>
#include <thread>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>
#include <vector>
#include <atomic>
#include <cstdint>
#include <algorithm>
#include "simdjson.h"
#include "reconnector.h"
#include "recorder.h"
#include "runtime.h" // g_shutdown, install_signal_handlers, now_millis, sleep_until_ms

// Both channels in one batch frame - a batch counts as one request against
// the 20-per-second rate limit. price.crypto.twap btcusd/60 is what the
// btc-updown-5m markets resolve on; price.crypto btcusd is the Chainlink
// spot price it averages. No "provider" pin: btcusd already defaults to
// Chainlink, and every payload's "source" field says who actually produced
// it.
static const std::string kSubscribeFrame =
    "{\"op\":\"subscribe\",\"rid\":\"s1\",\"subscriptions\":["
    "{\"channel\":\"price.crypto.twap\",\"filter\":{\"symbol\":\"btcusd\",\"window_seconds\":60}},"
    "{\"channel\":\"price.crypto\",\"filter\":{\"symbol\":\"btcusd\"}}"
    "]}";

// The secret is base64 and the passphrase is free text, so either could in
// principle contain a character that is special inside a JSON string (" or
// \). Splicing them in raw would then produce a broken auth frame. Escaping
// is what any JSON library would do; this covers the characters JSON
// requires escaping.
static std::string json_escape(const std::string& in) {
    std::string out;
    out.reserve(in.size());
    for (char c : in) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    // Any other control character: \u00XX form.
                    char buf[7];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out += c;
                }
        }
    }
    return out;
}

static const char* kDefaultUrl = "wss://ws-live-v2.polymarket.com/ws";

static void print_usage(const char* program) {
    std::cerr << "Usage: " << program << " [--verbose] [--url <ws-url>]\n"
              << "  --verbose   print every received message to stderr (slow:\n"
              << "              each print blocks the receiving thread)\n"
              << "  --url       connect somewhere other than " << kDefaultUrl << "\n"
              << "              (for testing reconnects against a local server)\n";
}

// PolyBolt's own close codes (table in docs/polybolt_subscription.md).
// Handed to the shared Reconnector, which acts on the answer.
static CloseAction classify_close(int code) {
    switch (code) {
        case 4001:  // credentials rejected - same credentials fail again
        case 4008:  // policy violation (a limit) - the request itself is wrong
            return CloseAction::Fatal;
        case 4003:  // server restarting
            return CloseAction::ServerDraining;
        default:    // 1006 network drop, 4002 slow/missed pongs, a failed
                    // connect (no code, 0), or anything unexpected
            return CloseAction::Backoff;
    }
}

// argc/argv: the words typed on the command line - see binance_ws.cpp.
int main(int argc, char* argv[]) {

    // Off by default: printing every message means a blocking write to the
    // terminal on the same thread that stamps recv_ts.
    bool verbose = false;
    std::string url = kDefaultUrl;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if (arg == "--verbose") {
            verbose = true;
        } else if (arg == "--url") {
            // The next word is the value. i + 1 < argc checks it exists, so
            // "./polybolt_ws --url" alone is an error, not a crash.
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

    // getenv returns nullptr when a variable isn't set. Check all three up
    // front and name the missing ones - but never print the values, here
    // or anywhere else in this program.
    const char* api_key    = std::getenv("POLY_API_KEY");
    const char* secret     = std::getenv("POLY_SECRET");
    const char* passphrase = std::getenv("POLY_PASSPHRASE");

    std::vector<std::string> missing;
    if (!api_key    || !*api_key)    missing.push_back("POLY_API_KEY");
    if (!secret     || !*secret)     missing.push_back("POLY_SECRET");
    if (!passphrase || !*passphrase) missing.push_back("POLY_PASSPHRASE");

    if (!missing.empty()) {
        std::cerr << "Missing environment variable(s):";
        for (const std::string& name : missing) {
            std::cerr << " " << name;
        }
        std::cerr << "\nLoad them first:  set -a; source .env; set +a" << std::endl;
        return 1;
    }

    // Built once here and reused on every (re)connect. rid "a1" is our
    // request id - the server echoes it on the reply.
    const std::string auth_frame =
        "{\"op\":\"auth\",\"rid\":\"a1\",\"auth\":{"
        "\"apiKey\":\""     + json_escape(api_key)    + "\","
        "\"secret\":\""     + json_escape(secret)     + "\","
        "\"passphrase\":\"" + json_escape(passphrase) + "\"}}";

    ix::WebSocket webSocket;
    webSocket.setUrl(url);

    // Reconnect decisions and the silence watchdog (see reconnector.h).
    // The callback reports to it; the main loop calls poll().
    //
    // 10s silence limit: both channels send one update per second, so 10s
    // of nothing is 20 missed messages - clearly broken, not quiet.
    //
    // healthy_on_first_message = false: a PolyBolt connection only counts
    // as working once logged in (an error ack is a message too). The
    // callback calls on_healthy() on "authed" instead.
    Reconnector reconnector(webSocket, ReconnectConfig{
        "polybolt",
        10000,
        classify_close,
        false,
    });

    // Only ever used on IXWebSocket's thread (inside the callback), so it
    // needs no locking.
    simdjson::ondemand::parser parser;

    // Starts the writer thread. Destination is std::cout, same as the other
    // recorders: stdout is the data channel and every log line goes to
    // std::cerr, so file naming and compression are a launch-time choice:
    //     ./polybolt_ws > Data/polybolt.jsonl
    //     ./polybolt_ws | gzip > Data/polybolt.jsonl.gz
    // Declared before setOnMessageCallback below, which captures it.
    Recorder recorder(std::cout);

    // Messages received since the last [stats] line. The callback adds 1,
    // the main thread reads and resets it - two threads, so atomic.
    std::atomic<uint64_t> msg_count{0};

    // verbose by VALUE (set once, never changes, so a copy is safe);
    // everything else by REFERENCE (shared with main).
    webSocket.setOnMessageCallback([&webSocket, &auth_frame, &parser, &recorder, &msg_count,
                                    &reconnector, verbose](const ix::WebSocketMessagePtr& msg) {
        if (msg->type == ix::WebSocketMessageType::Open) {
            // Every connection starts unauthenticated, so auth is sent on
            // every open - the first one and any reconnect alike.
            std::cerr << "Connection opened, sending auth..." << std::endl;
            webSocket.send(auth_frame);

        } else if (msg->type == ix::WebSocketMessageType::Message) {
            // Taken first, before any other work in the callback, so it is
            // as close as possible to when the message actually arrived.
            // Microseconds, matching the other recorders.
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

            // Recorded BEFORE parsing, like the other recorders, so capture
            // never depends on our parser succeeding. Every message is
            // recorded, acks included: a fresh authed/subscribed pair plus
            // snapshots with seq back at 1 is exactly what marks a
            // reconnect (a gap) in the data.
            //
            // Only messages we RECEIVE are recorded. The auth frame we send
            // never passes through here, so credentials can't end up in a
            // recording.
            std::string record = "{\"recv_ts\":" + std::to_string(recv_ts) +
                                 ",\"data\":";
            record.append(payload);
            record += "}\n";
            recorder.write(std::move(record));

            if (verbose) {
                std::cerr << "[recv] " << msg->str << std::endl;
            }

            // Subscribing has to wait for the server to confirm the login -
            // a subscribe that arrives first is rejected with
            // auth_required. So this reacts to the "authed" reply rather
            // than sending everything at once on open.
            //
            // Unlike the other recorders, this parse runs on every message,
            // not just under --verbose: it's what drives the login sequence.
            // At ~2 messages per second its cost is negligible.
            //
            // Acks carry an "op" field; price envelopes don't. get(op)
            // returns an error code instead of throwing when the field is
            // missing, so a price message simply isn't an ack - that's the
            // normal case, not a failure.
            try {
                simdjson::padded_string padded(msg->str);
                auto doc = parser.iterate(padded);

                std::string_view op;
                if (doc["op"].get_string().get(op) == simdjson::SUCCESS) {
                    if (op == "authed") {
                        std::cerr << "Authenticated, subscribing..." << std::endl;
                        webSocket.send(kSubscribeFrame);
                        // This connection works - the Reconnector resets
                        // the backoff once it has stayed up for 30s.
                        reconnector.on_healthy();
                    } else if (op == "error") {
                        // Always shown, verbose or not: an error ack (e.g.
                        // auth_invalid) means we're not getting data, and
                        // it's rare, so printing it costs nothing.
                        std::cerr << "[error ack] " << msg->str << std::endl;
                    }
                }
            } catch (const simdjson::simdjson_error& e) {
                // Not valid JSON at all. Shouldn't happen, but must never
                // escape into IXWebSocket's thread and kill the process.
                std::cerr << "Parse error: " << e.what() << std::endl;
            }

        } else if (msg->type == ix::WebSocketMessageType::Close) {
            // The close code says WHY - 4001 bad credentials, 4008 policy
            // violation, etc. (table in docs/polybolt_subscription.md).
            std::cerr << "Connection closed: code=" << msg->closeInfo.code
                      << " reason=" << msg->closeInfo.reason << std::endl;
            reconnector.on_disconnect(msg->closeInfo.code);

        } else if (msg->type == ix::WebSocketMessageType::Error) {
            // With automatic reconnection off, an Error here means a connect
            // attempt failed (DNS, TLS, refused handshake) and IXWebSocket's
            // thread has given up - so it's a disconnect too, with no close
            // code. http_status is set when the server refused the
            // handshake, e.g. 429 (too many requests) or 503 (unavailable).
            std::cerr << "Error: " << msg->errorInfo.reason
                      << " (http status " << msg->errorInfo.http_status << ")" << std::endl;
            reconnector.on_disconnect(0);
        }
    });

    // Ctrl+C / SIGTERM flip g_shutdown instead of killing the process.
    // Installed BEFORE start() so there is no moment where messages are
    // being queued but a Ctrl+C would still kill us outright.
    install_signal_handlers();
    reconnector.start();

    // The main thread does two jobs: every reconnect decision (through
    // reconnector.poll()), and the [stats] line every 5s (as in binance_ws -
    // expect ~10 msgs per line here, one live update per second on each
    // channel).
    //
    // It wakes at least every 200ms, so a disconnect or a silent feed is
    // acted on within 0.2s. sleep_until_ms returns false on Ctrl+C, so
    // shutdown stays instant.
    const int64_t stats_interval_ms = 5000;
    int64_t next_stats_ms = now_millis() + stats_interval_ms;
    uint64_t last_dropped = 0;
    int exit_code = 0;

    while (sleep_until_ms(std::min(next_stats_ms, now_millis() + 200))) {
        // false = a fatal close (e.g. 4001 bad credentials): stop.
        if (!reconnector.poll()) {
            exit_code = 1;
            break;
        }

        // --- [stats] line ---
        if (now_millis() >= next_stats_ms) {
            // exchange(0) reads and resets in one step, so no message
            // counted between a separate read and reset can be lost.
            uint64_t received = msg_count.exchange(0);

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

    // 1 after a fatal close, so whatever runs this program (a person, or
    // systemd on the server) can see it stopped because of a problem.
    return exit_code;
}
