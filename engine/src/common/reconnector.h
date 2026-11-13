#pragma once

// Reconnect decisions and the silence watchdog, shared by every feed.
//
// Split across two threads, the same way in every recorder:
//   - The websocket callback (IXWebSocket's thread) only REPORTS what
//     happened: on_message(), on_disconnect(code), on_healthy().
//   - The main thread calls poll() every ~200ms, and poll() makes every
//     decision: wait, reconnect, or give up.
//
// Reconnecting means stop() then start() on the ix::WebSocket, which must
// never happen on the callback's own thread - stop() waits for that thread
// to finish, so it would wait for itself forever. That's why the callback
// only reports.
//
// IXWebSocket's automatic reconnection must be OFF (the constructor turns
// it off): it would reconnect after any close, ignoring the close code,
// with no jitter.

#include <atomic>
#include <cstdint>
#include <string>
#include <ixwebsocket/IXWebSocket.h>
#include "common/backoff.h"

// What to do about a close, decided per feed from its close code.
enum class CloseAction {
    Fatal,           // retrying can't help - stop the program
    ServerDraining,  // reconnect once after a random 0-10s (PolyBolt 4003)
    Backoff,         // exponential backoff with full jitter
};

// Close code used for "we closed it ourselves because the feed went
// silent". Not a real websocket code (those are 1000+), so it can't clash.
constexpr int kSilenceCloseCode = -1;

// Default rule for feeds with no special close codes (Binance, CLOB):
// every close is worth retrying.
CloseAction always_backoff(int code);

struct ReconnectConfig {
    // Prefix for log lines, e.g. "polybolt", so logs from several
    // recorders can be told apart.
    std::string name;

    // Force a reconnect if no message arrives for this long. Chosen per
    // feed from its normal message rate.
    int64_t silence_timeout_ms;

    // How the feed's close codes map to actions.
    CloseAction (*classify)(int code) = always_backoff;

    // true: the first message on a connection proves it works (Binance,
    // CLOB). false: the feed calls on_healthy() itself at the right moment
    // (PolyBolt, once logged in - an error ack is a message too, but not a
    // working connection).
    bool healthy_on_first_message = true;

    // Reset the backoff once a connection has stayed healthy this long.
    // Resetting the instant it connects would let a server that accepts
    // and immediately drops us be retried at full speed forever.
    int64_t stable_after_ms = 30000;
};

class Reconnector {
public:
    // Turns off IXWebSocket's own automatic reconnection.
    Reconnector(ix::WebSocket& ws, ReconnectConfig config);

    // --- Called from the websocket callback (any thread) ---

    // A message arrived: the connection is alive.
    void on_message();

    // The connection closed (code from closeInfo), or a connect attempt
    // failed (code 0).
    void on_disconnect(int code);

    // The connection is properly working (see healthy_on_first_message).
    void on_healthy();

    // --- Called from the main thread only ---

    // First connect. Use instead of ws.start(), so the watchdog's clock
    // starts too.
    void start();

    // Run every ~200ms. Handles disconnects, the silence watchdog,
    // scheduled reconnects and the backoff reset. Returns false when a
    // fatal close means the program should stop.
    bool poll();

private:
    // A steady clock counts like a stopwatch: always forward, never
    // adjusted. The wall clock (system_clock) can jump when the server's
    // time is corrected, which would fake or hide a silence.
    static int64_t steady_ms();

    void schedule(int code, CloseAction action);
    void restart();

    ix::WebSocket& ws_;
    ReconnectConfig config_;

    // Written by the callback, read by poll() - so atomic.
    std::atomic<bool> disconnected_{false};
    std::atomic<int> last_close_code_{0};
    std::atomic<int64_t> last_message_ms_{0};   // steady_ms of the last message (or of start)
    std::atomic<int64_t> healthy_since_ms_{0};  // steady_ms it became healthy (0 = not yet)

    // Main thread only - no atomics needed.
    Backoff backoff_;
    bool reconnect_pending_ = false;
    int64_t reconnect_at_ms_ = 0;
};
