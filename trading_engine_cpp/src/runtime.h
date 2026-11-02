#pragma once

// Process-lifetime plumbing shared by every feed binary: clean shutdown on
// Ctrl+C / SIGTERM, and the clock/sleep helpers used for scheduling. Lives
// in one place so every feed shuts down identically.

#include <atomic>
#include <cstdint>

// Set by the signal handler, read by the main loop. A signal handler may
// only do async-signal-safe things: setting a lock-free atomic is on that
// list, taking a mutex or allocating memory is not. The handler is
// interrupting some thread mid-instruction - if that thread happened to
// hold the recorder's mutex, calling stop() there would deadlock against
// itself. So the handler raises a flag and nothing else; the main thread
// does the actual shutdown in ordinary code.
//
// extern = this only announces the variable; runtime.cpp creates it. If
// the header created it, every file including it would get its own
// separate, unconnected copy.
extern std::atomic<bool> g_shutdown;

// Turns a silent assumption into a compile error if it ever stops being true.
static_assert(std::atomic<bool>::is_always_lock_free,
              "g_shutdown must be lock-free to be safe in a signal handler");

// SIGINT is Ctrl+C; SIGTERM is what kill and systemd send, so the same
// clean shutdown works when this eventually runs as a service. Both just
// flip g_shutdown. SIGKILL (kill -9) cannot be caught by design, and will
// always lose whatever is still queued.
void install_signal_handlers();

int64_t now_millis();

// Sleep until target_ms, waking every 200ms to notice a shutdown request.
// Returns false if interrupted, true if we reached the target.
//
// Chunked rather than one sleep_for because a wait can be nearly five
// minutes long (just after a boundary in clob_ws) - Ctrl+C would look like
// a hang, and a second Ctrl+C kills the process outright, losing the
// queued records. std::min keeps the final chunk exact, so boundary
// precision is unchanged.
bool sleep_until_ms(int64_t target_ms);