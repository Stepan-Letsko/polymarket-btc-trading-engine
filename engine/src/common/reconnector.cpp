#include "common/reconnector.h"

#include <chrono>
#include <iostream>
#include <utility>

CloseAction always_backoff(int) {
    return CloseAction::Backoff;
}

Reconnector::Reconnector(ix::WebSocket& ws, ReconnectConfig config)
    : ws_(ws), config_(std::move(config)) {
    ws_.disableAutomaticReconnection();

    // IXWebSocket waits up to 60s for a connection handshake by default.
    // Connecting runs on its thread, and stop() waits for that thread - so
    // a watchdog stop() during a stuck handshake could hang the main loop
    // for up to a minute. 10s is plenty for a healthy server.
    ws_.setHandshakeTimeout(10);
}

int64_t Reconnector::steady_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

void Reconnector::on_message() {
    last_message_ms_ = steady_ms();
    if (config_.healthy_on_first_message && healthy_since_ms_ == 0) {
        healthy_since_ms_ = last_message_ms_.load();
    }
}

void Reconnector::on_disconnect(int code) {
    // Code stored BEFORE the flag is raised, so when poll() sees the flag,
    // the matching code is already there.
    last_close_code_ = code;
    disconnected_ = true;
}

void Reconnector::on_healthy() {
    healthy_since_ms_ = steady_ms();
}

void Reconnector::start() {
    // The watchdog measures silence from the moment we start trying, so a
    // connection that opens but never sends anything is caught too.
    last_message_ms_ = steady_ms();
    healthy_since_ms_ = 0;
    ws_.start();
}

void Reconnector::restart() {
    // stop() waits for IXWebSocket's thread to finish. If the connection
    // was still open (the watchdog case), that thread reports our own
    // close as a disconnect on its way out - but by the time stop()
    // returns, it already has, so clearing the flag here discards exactly
    // that report and nothing else.
    ws_.stop();
    disconnected_ = false;
    start();
}

void Reconnector::schedule(int code, CloseAction action) {
    healthy_since_ms_ = 0;  // this connection is gone

    int64_t delay_ms = (action == CloseAction::ServerDraining)
        ? backoff_.random_delay_ms(10000)
        : backoff_.next_delay_ms();

    reconnect_pending_ = true;
    reconnect_at_ms_ = steady_ms() + delay_ms;

    std::cerr << "--- [" << config_.name << "] reconnecting in " << delay_ms / 1000.0 << "s ("
              << (code == kSilenceCloseCode ? "silence" : "close code " + std::to_string(code))
              << ", ";
    if (action == CloseAction::ServerDraining) {
        std::cerr << "server draining: random 0-10s";
    } else {
        std::cerr << "backoff attempt " << backoff_.attempt();
    }
    std::cerr << ") ---" << std::endl;
}

bool Reconnector::poll() {
    int64_t now_ms = steady_ms();

    // --- A disconnect was reported ---
    // exchange(false) reads and clears in one step. Cleared even while a
    // reconnect is already scheduled, so a second report of the same drop
    // can't schedule a second reconnect.
    if (disconnected_.exchange(false) && !reconnect_pending_) {
        int code = last_close_code_;
        CloseAction action = config_.classify(code);
        if (action == CloseAction::Fatal) {
            std::cerr << "--- [" << config_.name << "] FATAL: close code " << code
                      << " - retrying can't fix this, stopping ---" << std::endl;
            return false;
        }
        schedule(code, action);
    }

    // --- Silence watchdog ---
    // Only while a connection is meant to be up, not while we're already
    // waiting to reconnect.
    if (!reconnect_pending_ && now_ms - last_message_ms_ >= config_.silence_timeout_ms) {
        std::cerr << "--- [" << config_.name << "] no messages for "
                  << (now_ms - last_message_ms_) / 1000.0 << "s, forcing reconnect ---" << std::endl;
        // Close the silent connection now, and clear the close report it
        // makes on the way out - this reconnect is ours, decided here.
        ws_.stop();
        disconnected_ = false;
        schedule(kSilenceCloseCode, CloseAction::Backoff);
    }

    // --- Scheduled reconnect is due ---
    if (reconnect_pending_ && now_ms >= reconnect_at_ms_) {
        reconnect_pending_ = false;
        std::cerr << "--- [" << config_.name << "] reconnecting now ---" << std::endl;
        restart();
    }

    // --- Healthy long enough: reset the backoff ---
    int64_t healthy_since = healthy_since_ms_;
    if (backoff_.attempt() > 0 && healthy_since > 0 &&
        now_ms - healthy_since >= config_.stable_after_ms) {
        std::cerr << "--- [" << config_.name << "] connection stable, backoff reset ---" << std::endl;
        backoff_.reset();
    }

    return true;
}
