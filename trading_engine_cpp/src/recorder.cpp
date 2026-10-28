#include "recorder.h"

Recorder::Recorder(std::ostream& out, size_t max_queued)
    : out_(out), max_queued_(max_queued) {
    thread_ = std::thread(&Recorder::run, this);
}

Recorder::~Recorder() {
    stop();
}

void Recorder::write(std::string line) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.size() >= max_queued_) {
            // Overloaded. Drop rather than grow without limit - losing
            // bounded, counted data beats running out of memory, and a
            // non-zero counter tells us it happened.
            dropped_.fetch_add(1);
            return;
        }
        // std::move hands over the string's buffer rather than copying
        // its characters, so this stays cheap even for large messages.
        queue_.push_back(std::move(line));
    }
    cv_.notify_one();
}

uint64_t Recorder::dropped() const {
    return dropped_.load();
}

void Recorder::stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stopping_) return;   // already stopped; stop() is idempotent
        stopping_ = true;
    }
    cv_.notify_one();
    if (thread_.joinable()) {
        thread_.join();   // waits for run() to finish draining
    }
}

void Recorder::run() {
    std::vector<std::string> batch;

    while (true) {
        {
            std::unique_lock<std::mutex> lock(mutex_);

            // Sleep until there's something to write or we're shutting
            // down. The predicate form also absorbs spurious wakeups,
            // so an empty batch below can only mean "stopping".
            cv_.wait(lock, [this] { return !queue_.empty() || stopping_; });

            // Take the whole queue in one go by swapping pointers - no
            // copying, and the producer gets an empty queue back
            // immediately rather than waiting for us to write.
            batch.swap(queue_);
        }   // lock released here - we do the slow part unlocked

        if (batch.empty()) {
            break;   // woken with nothing queued => stopping, and drained
        }

        for (const std::string& line : batch) {
            out_ << line;
        }
        out_.flush();   // once per batch, not once per line
        batch.clear();
    }

    out_.flush();
}
