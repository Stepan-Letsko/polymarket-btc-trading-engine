#pragma once

#include <string>
#include <cstdint>

struct Market {
    std::string token_up;
    std::string token_down;
    std::string condition_id;   // 0x-prefixed; market_resolved events key off this

    // A fetch that failed returns a default-constructed Market, so
    // callers must check before committing to it - otherwise an HTTP
    // failure silently replaces a working market with empty tokens.
    bool valid() const {
        return !token_up.empty() && !token_down.empty() && !condition_id.empty();
    }
};

// The 5-minute window currently running, as a unix timestamp.
int64_t current_boundary();

// Fetch the market for a specific 5-minute window. Lets callers ask for
// an upcoming window (current_boundary() + 300) before it starts.
Market get_market_for_boundary(int64_t boundary_ts);

// Convenience wrapper: the window running right now.
Market get_current_market();
