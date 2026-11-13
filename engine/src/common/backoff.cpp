#include "common/backoff.h"

#include <algorithm>

Backoff::Backoff(int64_t base_ms, int64_t cap_ms)
    : Backoff(base_ms, cap_ms, std::random_device{}()) {}
    // ^ delegates to the seeded constructor below; random_device asks the
    //   operating system for a genuinely unpredictable seed

Backoff::Backoff(int64_t base_ms, int64_t cap_ms, uint64_t seed)
    : base_ms_(base_ms), cap_ms_(cap_ms), limit_ms_(base_ms), rng_(seed) {}

int64_t Backoff::next_delay_ms() {
    // uniform_int_distribution(a, b) picks any whole number from a to b
    // inclusive, each equally likely.
    std::uniform_int_distribution<int64_t> pick(0, limit_ms_);
    int64_t delay = pick(rng_);

    // Double for next time, but never past the cap. Comparing before
    // doubling means limit_ms_ can never overflow, however many attempts.
    limit_ms_ = (limit_ms_ >= cap_ms_ / 2) ? cap_ms_ : limit_ms_ * 2;
    ++attempt_;
    return delay;
}

int64_t Backoff::random_delay_ms(int64_t max_ms) {
    std::uniform_int_distribution<int64_t> pick(0, max_ms);
    return pick(rng_);
}

void Backoff::reset() {
    limit_ms_ = base_ms_;
    attempt_ = 0;
}

int Backoff::attempt() const {
    return attempt_;
}

int64_t Backoff::current_limit_ms() const {
    return limit_ms_;
}
