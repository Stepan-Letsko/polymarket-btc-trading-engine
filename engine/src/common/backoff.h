#pragma once

// Reconnect wait times, shared by every feed: "exponential backoff with
// full jitter", the standard scheme (and the one PolyBolt's docs ask for).
//
// Exponential backoff: each failed attempt doubles the LONGEST we're
// allowed to wait - 1s, 2s, 4s, 8s, 16s, then capped at 30s - so a server
// that's down isn't hammered every second.
//
// Full jitter: the ACTUAL wait is a random amount between 0 and that limit.
// Without it, every client dropped at the same moment (e.g. a server
// restart) would retry at exactly the same instants, all at once, over and
// over. Randomness spreads them out.
//
// reset() after a connection is properly working again, so one drop next
// week doesn't start at a 30s wait because of a drop today.
//
// Not thread-safe: meant to be owned and used by one thread (the main
// thread, which makes the reconnect decisions).

#include <cstdint>
#include <random>

class Backoff {
public:
    // Seeded from std::random_device, so each run's waits differ.
    explicit Backoff(int64_t base_ms = 1000, int64_t cap_ms = 30000);

    // Same, with a fixed seed, so a test gets the same "random" waits every
    // run and its results are repeatable.
    Backoff(int64_t base_ms, int64_t cap_ms, uint64_t seed);

    // The wait before the next attempt: random in [0, current limit]. Then
    // doubles the limit (up to the cap) for the attempt after.
    int64_t next_delay_ms();

    // A one-off random wait in [0, max_ms], not part of the doubling - for
    // cases with their own rule, like PolyBolt's "server restarting:
    // reconnect after a random 0-10s". Uses the same random generator.
    int64_t random_delay_ms(int64_t max_ms);

    // Back to the first limit (base_ms) and attempt 0.
    void reset();

    // How many next_delay_ms() calls since the last reset - for logging,
    // e.g. "reconnecting (attempt 3)".
    int attempt() const;

    // The limit the NEXT next_delay_ms() will draw under.
    int64_t current_limit_ms() const;

private:
    int64_t base_ms_;
    int64_t cap_ms_;
    int64_t limit_ms_;   // grows base -> cap; kept directly rather than
                         // computed as base * 2^attempt, which would
                         // overflow after enough attempts
    int attempt_ = 0;
    std::mt19937_64 rng_;  // standard high-quality random number generator
};
