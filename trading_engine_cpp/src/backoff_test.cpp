// Tests for Backoff. Fixed seeds, so every run sees the same "random"
// numbers and a failure is repeatable.
//
// Build and run:
//   cd build && cmake .. && cmake --build . && ./backoff_test
// Exits 0 if every check passed, 1 otherwise.

#include "backoff.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <string>
#include <vector>

static int failures = 0;

static void check(bool passed, const std::string& what) {
    if (passed) {
        std::cout << "  pass  " << what << "\n";
    } else {
        std::cout << "  FAIL  " << what << "\n";
        ++failures;
    }
}

// The limit doubles from base to cap and then stays at the cap.
static void test_limit_doubles_then_caps() {
    std::cout << "test_limit_doubles_then_caps\n";

    Backoff backoff(1000, 30000, 1);
    std::vector<int64_t> expected = {1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000};
    bool all_match = true;
    for (int64_t want : expected) {
        if (backoff.current_limit_ms() != want) {
            all_match = false;
            std::cout << "        expected limit " << want
                      << ", got " << backoff.current_limit_ms() << "\n";
        }
        backoff.next_delay_ms();
    }
    check(all_match, "limits go 1s, 2s, 4s, 8s, 16s, then 30s cap");
}

// Every wait is inside [0, the limit it was drawn under] - never negative,
// never past the limit.
static void test_delays_within_limit() {
    std::cout << "test_delays_within_limit\n";

    Backoff backoff(1000, 30000, 2);
    bool all_in_range = true;
    for (int i = 0; i < 10000; ++i) {
        if (i % 10 == 0) {
            backoff.reset();  // cover every attempt number repeatedly
        }
        int64_t limit = backoff.current_limit_ms();
        int64_t delay = backoff.next_delay_ms();
        if (delay < 0 || delay > limit) {
            all_in_range = false;
        }
    }
    check(all_in_range, "10000 delays all within [0, limit]");
}

// Jitter must actually be random: across many draws at the same limit, the
// waits should spread over the whole range, not cluster or repeat.
static void test_jitter_spreads() {
    std::cout << "test_jitter_spreads\n";

    Backoff backoff(1000, 30000, 3);
    int64_t lowest = 1000;
    int64_t highest = 0;
    for (int i = 0; i < 1000; ++i) {
        backoff.reset();  // always draw under the 1s limit
        int64_t delay = backoff.next_delay_ms();
        lowest = std::min(lowest, delay);
        highest = std::max(highest, delay);
    }
    std::cout << "        1000 draws under a 1000ms limit: lowest=" << lowest
              << " highest=" << highest << "\n";
    check(lowest < 100 && highest > 900, "waits spread across the whole 0-1000ms range");
}

// reset() returns to the first limit and attempt 0.
static void test_reset() {
    std::cout << "test_reset\n";

    Backoff backoff(1000, 30000, 4);
    for (int i = 0; i < 5; ++i) {
        backoff.next_delay_ms();
    }
    check(backoff.attempt() == 5, "attempt counts calls (5)");
    backoff.reset();
    check(backoff.attempt() == 0 && backoff.current_limit_ms() == 1000,
          "reset() goes back to attempt 0 and a 1s limit");
}

// A long outage (thousands of attempts) must not overflow the limit into
// a negative or huge number.
static void test_no_overflow() {
    std::cout << "test_no_overflow\n";

    Backoff backoff(1000, 30000, 5);
    for (int i = 0; i < 100000; ++i) {
        backoff.next_delay_ms();
    }
    check(backoff.current_limit_ms() == 30000, "limit still exactly 30s after 100000 attempts");
}

// random_delay_ms stays inside [0, max] and leaves the doubling alone.
static void test_random_delay() {
    std::cout << "test_random_delay\n";

    Backoff backoff(1000, 30000, 6);
    bool all_in_range = true;
    for (int i = 0; i < 10000; ++i) {
        int64_t delay = backoff.random_delay_ms(10000);
        if (delay < 0 || delay > 10000) {
            all_in_range = false;
        }
    }
    check(all_in_range, "10000 random_delay_ms(10000) all within [0, 10000]");
    check(backoff.attempt() == 0 && backoff.current_limit_ms() == 1000,
          "random_delay_ms doesn't advance the backoff");
}

int main() {
    test_limit_doubles_then_caps();
    test_delays_within_limit();
    test_jitter_spreads();
    test_reset();
    test_no_overflow();
    test_random_delay();

    if (failures == 0) {
        std::cout << "\nall checks passed\n";
        return 0;
    }
    std::cout << "\n" << failures << " check(s) failed\n";
    return 1;
}
