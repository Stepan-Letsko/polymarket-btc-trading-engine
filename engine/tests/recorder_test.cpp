// Tests for Recorder. Writes into an ostringstream rather than a file or
// std::cout so the test can inspect exactly what came out the other side.
//
// Build and run:
//   cd build && cmake .. && cmake --build . && ./recorder_test
// Exits 0 if every check passed, 1 otherwise.

#include "recording/recorder.h"

#include <cstdint>
#include <iostream>
#include <set>
#include <sstream>
#include <string>
#include <thread>
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

// Split captured output back into the individual lines that were written.
static std::vector<std::string> split_lines(const std::string& text) {
    std::vector<std::string> lines;
    std::istringstream stream(text);
    std::string line;
    while (std::getline(stream, line)) {
        lines.push_back(line);
    }
    return lines;
}

// The core guarantee: every line handed to the Recorder comes out the other
// side, exactly once, even with several threads writing concurrently. Each
// line is unique so duplicates and losses are both detectable.
static void test_nothing_is_lost() {
    std::cout << "test_nothing_is_lost\n";

    const int thread_count = 4;
    const int per_thread = 2000;
    const size_t expected = static_cast<size_t>(thread_count) * per_thread;

    std::ostringstream out;
    {
        Recorder recorder(std::make_unique<StreamSink>(out));

        std::vector<std::thread> writers;
        for (int t = 0; t < thread_count; ++t) {
            writers.emplace_back([&recorder, t] {
                for (int i = 0; i < per_thread; ++i) {
                    recorder.write(0, "t" + std::to_string(t) + "-" + std::to_string(i) + "\n");
                }
            });
        }
        for (std::thread& writer : writers) {
            writer.join();
        }

        recorder.stop();
        check(recorder.dropped() == 0, "nothing dropped at the default queue size");
    }

    std::vector<std::string> lines = split_lines(out.str());
    check(lines.size() == expected, "all " + std::to_string(expected) + " lines were written");

    std::set<std::string> unique(lines.begin(), lines.end());
    check(unique.size() == lines.size(), "no line was written twice");
}

// Correct shutdown must be the default, not something the caller has to
// remember. Nothing calls stop() here - the destructor has to drain.
static void test_destructor_drains() {
    std::cout << "test_destructor_drains\n";

    const size_t count = 500;
    std::ostringstream out;
    {
        Recorder recorder(std::make_unique<StreamSink>(out));
        for (size_t i = 0; i < count; ++i) {
            recorder.write(0, "line\n");
        }
    }

    check(split_lines(out.str()).size() == count,
          "destructor drained the queue with no explicit stop()");
}

// stop() gets called twice in real use: once by a signal handler, once by the
// destructor. The second call must be a no-op, not a double join.
static void test_stop_is_repeatable() {
    std::cout << "test_stop_is_repeatable\n";

    std::ostringstream out;
    {
        Recorder recorder(std::make_unique<StreamSink>(out));
        recorder.write(0, "one\n");
        recorder.stop();
        recorder.stop();
        recorder.stop();
        check(split_lines(out.str()).size() == 1, "repeated stop() calls are harmless");
    }
}

// With a deliberately tiny queue the writer thread cannot keep up, so lines
// get dropped. Which ones is timing-dependent, but the accounting is not:
// written + dropped must equal submitted. A line may never vanish silently.
static void test_bounded_queue_accounting() {
    std::cout << "test_bounded_queue_accounting\n";

    const size_t total = 50000;
    std::ostringstream out;
    uint64_t dropped = 0;
    {
        Recorder recorder(std::make_unique<StreamSink>(out), 16);
        for (size_t i = 0; i < total; ++i) {
            recorder.write(0, "x\n");
        }
        recorder.stop();
        dropped = recorder.dropped();
    }

    size_t written = split_lines(out.str()).size();
    std::cout << "        submitted=" << total << " written=" << written
              << " dropped=" << dropped << "\n";
    check(written + dropped == total, "written + dropped == submitted");
}

// Stands in for a sink with a disk problem: refuses every second record.
class FlakySink : public Sink {
public:
    explicit FlakySink(std::ostream& out) : out_(out) {}
    bool write(const Record& record) override {
        if (++calls_ % 2 == 0) {
            return false;
        }
        out_ << record.line;
        return true;
    }
    uint64_t flush() override {
        out_.flush();
        return 0;
    }

private:
    std::ostream& out_;
    uint64_t calls_ = 0;
};

// A record the sink refuses must show up in dropped(), exactly once - the
// same "never vanish silently" rule as the queue, applied to the disk side.
static void test_sink_failures_count_as_dropped() {
    std::cout << "test_sink_failures_count_as_dropped\n";

    const size_t total = 1000;
    std::ostringstream out;
    uint64_t dropped = 0;
    {
        Recorder recorder(std::make_unique<FlakySink>(out));
        for (size_t i = 0; i < total; ++i) {
            recorder.write(0, "x\n");
        }
        recorder.stop();
        dropped = recorder.dropped();
    }

    size_t written = split_lines(out.str()).size();
    check(written == total / 2, "only the accepted half was written");
    check(dropped == total / 2, "every refused record was counted as dropped");
}

int main() {
    test_nothing_is_lost();
    test_destructor_drains();
    test_stop_is_repeatable();
    test_bounded_queue_accounting();
    test_sink_failures_count_as_dropped();

    if (failures == 0) {
        std::cout << "\nall checks passed\n";
        return 0;
    }
    std::cout << "\n" << failures << " check(s) failed\n";
    return 1;
}
