// Tests for HourlyFileSink. Writes into a throwaway folder under the
// system temp directory with made-up timestamps, so crossing an hour takes
// no waiting.
//
// Build and run:
//   cd build && cmake .. && cmake --build . && ./hourly_file_sink_test
// Exits 0 if every check passed, 1 otherwise.

#include "hourly_file_sink.h"

#include <csignal>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include <sys/resource.h>  // setrlimit, for faking a full disk
#include <unistd.h>        // getpid

namespace fs = std::filesystem;

static int failures = 0;

static void check(bool passed, const std::string& what) {
    if (passed) {
        std::cout << "  pass  " << what << "\n";
    } else {
        std::cout << "  FAIL  " << what << "\n";
        ++failures;
    }
}

// 2026-11-04 14:00:00 UTC, in microseconds since 1970.
constexpr int64_t kH14 = 1793800800LL * 1'000'000;
constexpr int64_t kHour = 3'600'000'000;

static std::string read_file(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    std::ostringstream text;
    text << in.rdbuf();
    return text.str();
}

// A fresh empty folder for each test, so one test's files can't affect
// another's. The process id keeps two runs at once from colliding.
static fs::path fresh_dir(const std::string& name) {
    fs::path dir = fs::temp_directory_path() /
                   ("hourly_sink_test_" + std::to_string(getpid())) / name;
    fs::remove_all(dir);
    fs::create_directories(dir);
    return dir;
}

static void test_same_hour_one_file() {
    std::cout << "test_same_hour_one_file\n";
    fs::path base = fresh_dir("same_hour");
    {
        HourlyFileSink sink(base, "feed");
        check(sink.write({kH14 + 1, "a\n"}), "first record accepted");
        check(sink.write({kH14 + 2, "b\n"}), "second record accepted");
        check(sink.flush() == 0, "flush reports nothing lost");

        // Visible on disk straight after flush, not only after closing.
        check(read_file(base / "feed" / "feed_2026-11-04T14.jsonl") == "a\nb\n",
              "both lines in the 14:00 file after flush");
    }
    check(std::distance(fs::directory_iterator(base / "feed"), fs::directory_iterator{}) == 1,
          "exactly one file was created");
}

// The last microsecond of 13:00 and the first of 14:00 must land on
// opposite sides of the cut.
static void test_hour_boundary() {
    std::cout << "test_hour_boundary\n";
    fs::path base = fresh_dir("boundary");
    {
        HourlyFileSink sink(base, "feed");
        sink.write({kH14 - 1, "last of 13\n"});
        sink.write({kH14, "first of 14\n"});
        sink.write({kH14 + kHour - 1, "last of 14\n"});
        sink.write({kH14 + kHour, "first of 15\n"});
        check(sink.flush() == 0, "flush reports nothing lost");
    }
    fs::path dir = base / "feed";
    check(read_file(dir / "feed_2026-11-04T13.jsonl") == "last of 13\n", "13:00 file holds only 13:59:59.999999");
    check(read_file(dir / "feed_2026-11-04T14.jsonl") == "first of 14\nlast of 14\n", "14:00 file holds its whole hour");
    check(read_file(dir / "feed_2026-11-04T15.jsonl") == "first of 15\n", "15:00 file starts exactly on the hour");
}

// A restart in the same hour must add to the file, not wipe it.
static void test_reopen_appends() {
    std::cout << "test_reopen_appends\n";
    fs::path base = fresh_dir("reopen");
    {
        HourlyFileSink sink(base, "feed");
        sink.write({kH14 + 1, "before restart\n"});
        sink.flush();
    }
    {
        HourlyFileSink sink(base, "feed");
        sink.write({kH14 + 2, "after restart\n"});
        sink.flush();
    }
    check(read_file(base / "feed" / "feed_2026-11-04T14.jsonl") == "before restart\nafter restart\n",
          "second run appended to the first run's file");
}

// Records left in the buffer are still written when the sink is destroyed
// (the Recorder normally flushes first; this is the safety net).
static void test_destructor_writes_rest() {
    std::cout << "test_destructor_writes_rest\n";
    fs::path base = fresh_dir("destructor");
    {
        HourlyFileSink sink(base, "feed");
        sink.write({kH14, "unflushed\n"});
    }
    check(read_file(base / "feed" / "feed_2026-11-04T14.jsonl") == "unflushed\n",
          "destructor wrote the unflushed record");
}

// A folder that can't be created: write() says no and nothing crashes.
// The "base folder" is really a plain file, so nothing can go inside it -
// unlike a permissions trick, this fails even when run as root.
static void test_unwritable_folder() {
    std::cout << "test_unwritable_folder\n";
    fs::path base = fresh_dir("unwritable");
    fs::path blocker = base / "not_a_folder";
    std::ofstream(blocker) << "x";

    HourlyFileSink sink(blocker, "feed");
    check(!sink.write({kH14, "a\n"}), "write returns false");
    check(sink.flush() == 0, "nothing was accepted, so flush has nothing to report");
}

// The disk "fills up" part way through a buffer. Every record must be
// counted exactly: the ones fully on disk saved, the rest reported lost.
// Faked by telling the OS this process may not grow any file past 1000
// bytes (RLIMIT_FSIZE) - writes beyond that fail like a full disk would.
static void test_disk_full_counts_exactly() {
    std::cout << "test_disk_full_counts_exactly\n";
    fs::path base = fresh_dir("disk_full");

    // The limit covers EVERY file this process writes - including its own
    // output, if that is redirected to a file already past 1000 bytes. So
    // get our output out of the way first, and reset it afterwards (below).
    // The sink's "write failed" line may be swallowed the same way; the
    // checks don't depend on it.
    std::cout.flush();
    std::cerr.flush();

    // Going over the limit normally also kills the process with SIGXFSZ;
    // ignoring it turns that into a plain write error instead.
    std::signal(SIGXFSZ, SIG_IGN);
    rlimit old_limit{};
    getrlimit(RLIMIT_FSIZE, &old_limit);
    rlimit small = old_limit;
    small.rlim_cur = 1000;
    setrlimit(RLIMIT_FSIZE, &small);

    // 100 records of exactly 25 bytes = 2500 bytes. Only the first 1000
    // bytes fit, which is exactly 40 whole records.
    const std::string line = std::string(24, 'x') + "\n";
    uint64_t lost = 0;
    {
        HourlyFileSink sink(base, "feed");
        for (int i = 0; i < 100; ++i) {
            sink.write({kH14 + i, line});
        }
        lost = sink.flush();
    }

    setrlimit(RLIMIT_FSIZE, &old_limit);
    std::signal(SIGXFSZ, SIG_DFL);
    // A stream that failed once stays switched off until cleared.
    std::cout.clear();
    std::cerr.clear();

    std::string text = read_file(base / "feed" / "feed_2026-11-04T14.jsonl");
    std::cout << "        bytes on disk=" << text.size() << " lost=" << lost << "\n";
    check(text.size() == 1000, "file stopped at the 1000 byte limit");
    check(lost == 60, "exactly the 60 records that didn't fit were reported lost");
}

// A crash left the file ending in half a record. Our first record must
// start on its own line, not be glued onto the broken one. A file that
// already ends cleanly must be left exactly as it was.
static void test_half_line_repaired() {
    std::cout << "test_half_line_repaired\n";
    fs::path base = fresh_dir("half_line");
    fs::path dir = base / "feed";
    fs::create_directories(dir);
    std::ofstream(dir / "feed_2026-11-04T14.jsonl") << "complete\nhal";  // no newline at the end
    std::ofstream(dir / "feed_2026-11-04T15.jsonl") << "complete\n";
    {
        HourlyFileSink sink(base, "feed");
        sink.write({kH14, "next\n"});
        sink.write({kH14 + kHour, "next\n"});
        sink.flush();
    }
    check(read_file(dir / "feed_2026-11-04T14.jsonl") == "complete\nhal\nnext\n",
          "newline added after the half line, then our record");
    check(read_file(dir / "feed_2026-11-04T15.jsonl") == "complete\nnext\n",
          "a cleanly ending file got no extra newline");
}

// The server clock gets corrected backwards mid-hour. Records that now
// look like 13:00 keep going into the 14:00 file - files never go back.
static void test_clock_backwards() {
    std::cout << "test_clock_backwards\n";
    fs::path base = fresh_dir("backwards");
    {
        HourlyFileSink sink(base, "feed");
        sink.write({kH14 + 10, "14:00\n"});
        sink.write({kH14 - 30 * 60 * 1'000'000LL, "looks like 13:30\n"});
        sink.write({kH14 + 20, "14:00 again\n"});
        check(sink.flush() == 0, "flush reports nothing lost");
    }
    fs::path dir = base / "feed";
    check(read_file(dir / "feed_2026-11-04T14.jsonl") == "14:00\nlooks like 13:30\n14:00 again\n",
          "all three records in the 14:00 file, in arrival order");
    check(!fs::exists(dir / "feed_2026-11-04T13.jsonl"), "no 13:00 file was reopened");
}

static size_t count(const std::string& text, const std::string& word) {
    size_t n = 0;
    for (size_t at = text.find(word); at != std::string::npos; at = text.find(word, at + 1)) {
        ++n;
    }
    return n;
}

// 100 failures in a row print one error line, not 100. When writing
// works again, one line says so, with the number lost in between.
static void test_errors_logged_once() {
    std::cout << "test_errors_logged_once\n";
    fs::path base = fresh_dir("quiet_errors");
    fs::path blocker = base / "not_a_folder";
    std::ofstream(blocker) << "x";

    // Catch what the sink prints to stderr so the test can count it.
    std::ostringstream log;
    std::streambuf* real_cerr = std::cerr.rdbuf(log.rdbuf());
    bool all_refused = true;
    bool recovered_write = false;
    {
        HourlyFileSink sink(blocker, "feed");
        for (int i = 0; i < 100; ++i) {
            all_refused = !sink.write({kH14 + i, "a\n"}) && all_refused;
        }

        fs::remove(blocker);  // the problem goes away
        recovered_write = sink.write({kH14 + 100, "back\n"});
        sink.flush();
    }
    std::cerr.rdbuf(real_cerr);

    std::string text = log.str();
    check(all_refused, "all 100 writes refused while the folder was blocked");
    check(count(text, "cannot create") == 1, "one error line for 100 failures");
    check(recovered_write, "writing works once the folder can be created");
    check(count(text, "writing works again; 100 records were lost meanwhile") == 1,
          "one recovery line, with the exact number lost");
    check(read_file(blocker / "feed" / "feed_2026-11-04T14.jsonl") == "back\n",
          "the record after recovery is on disk");
}

int main() {
    test_same_hour_one_file();
    test_hour_boundary();
    test_reopen_appends();
    test_destructor_writes_rest();
    test_unwritable_folder();
    test_disk_full_counts_exactly();
    test_half_line_repaired();
    test_clock_backwards();
    test_errors_logged_once();

    fs::remove_all(fs::temp_directory_path() / ("hourly_sink_test_" + std::to_string(getpid())));

    if (failures == 0) {
        std::cout << "\nall checks passed\n";
        return 0;
    }
    std::cout << "\n" << failures << " check(s) failed\n";
    return 1;
}
