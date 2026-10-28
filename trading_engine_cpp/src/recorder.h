#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

// Moves the actual writing off whatever thread produced the data.
//
// The problem this exists to solve: a websocket callback runs on the
// same thread that has to read the next incoming message. If it writes
// to disk directly, a slow write stalls reception - which is exactly
// what happened in the Python recorder, where flushing per message left
// ~29% of messages arriving 1-18 seconds late.
//
// So write() does no I/O at all. It hands the line to a queue and
// returns; a dedicated thread drains that queue and does the writing,
// taking as long as it needs without anyone waiting on it.
class Recorder {
public:
    // max_queued caps memory use if the writer can't keep up. Past that
    // point lines are dropped and counted rather than growing until the
    // process runs out of memory - a visible, bounded failure instead of
    // a crash.
    explicit Recorder(std::ostream& out, size_t max_queued = 100000);
    ~Recorder();

    Recorder(const Recorder&) = delete;
    Recorder& operator=(const Recorder&) = delete;

    // Safe to call from any thread. Never blocks on I/O.
    void write(std::string line);

    // How many lines were discarded because the queue was full.
    uint64_t dropped() const;

    // Drain whatever is still queued, then stop the writer thread.
    // Idempotent, so it's safe to call explicitly on shutdown and again
    // from the destructor.
    void stop();

private:
    void run();   // the writer thread's loop

    std::ostream& out_;
    size_t max_queued_;

    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<std::string> queue_;
    bool stopping_ = false;

    std::atomic<uint64_t> dropped_{0};
    std::thread thread_;
};
