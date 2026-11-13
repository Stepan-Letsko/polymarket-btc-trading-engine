#include "common/runtime.h"

#include <algorithm>
#include <chrono>
#include <csignal>
#include <thread>

std::atomic<bool> g_shutdown{false};        // the actual switch, created once

static void handle_signal(int) {            // static = only visible in this file
    g_shutdown = true;
}

void install_signal_handlers() {
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);
}

int64_t now_millis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

bool sleep_until_ms(int64_t target_ms) {
    while (!g_shutdown) {
        int64_t remaining = target_ms - now_millis();
        if (remaining <= 0) {
            return true;
        }
        std::this_thread::sleep_for(
            std::chrono::milliseconds(std::min<int64_t>(remaining, 200)));
    }
    return false;}
