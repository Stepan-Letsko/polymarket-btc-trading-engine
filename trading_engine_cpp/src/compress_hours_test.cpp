// Tests for compress_hours' pass logic (compress_pass.h). Works in a
// throwaway folder; "now" and every file's modified time are set by the
// test, so no waiting.
//
// Build and run:
//   cd build && cmake .. && cmake --build . && ./compress_hours_test
// Exits 0 if every check passed, 1 otherwise.

#include "compress_pass.h"
#include "hourly_file_sink.h"

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include <sys/time.h>  // utimes, to set a file's modified time
#include <unistd.h>    // getpid, close

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

// 2026-11-04 14:00:00 UTC in seconds.
constexpr int64_t kH14 = 1793800800;
constexpr int64_t kMin = 60;

static fs::path fresh_dir(const std::string& name) {
    fs::path dir = fs::temp_directory_path() /
                   ("compress_hours_test_" + std::to_string(getpid())) / name;
    fs::remove_all(dir);
    fs::create_directories(dir);
    return dir;
}

// Create a file and set its "last modified" time.
static fs::path make_file(const fs::path& path, int64_t modified_seconds, const std::string& content = "{\"x\":1}\n") {
    fs::create_directories(path.parent_path());
    std::ofstream(path) << content;
    timeval times[2] = {{static_cast<time_t>(modified_seconds), 0}, {static_cast<time_t>(modified_seconds), 0}};
    utimes(path.c_str(), times);
    return path;
}

static bool zst_exists(const fs::path& path) {
    return fs::exists(path.string() + ".zst");
}

static void test_parse_names() {
    std::cout << "test_parse_names\n";
    check(parse_hour_file_name("binance_2026-11-04T14.jsonl", "binance") == kH14, "a normal name gives its hour");
    check(parse_hour_file_name("binance_2028-02-29T00.jsonl", "binance").has_value(), "29 Feb in a leap year is accepted");

    const char* rejected[] = {
        "clob_2026-11-04T14.jsonl",              // another feed's file
        "binance_2026-11-04T14.jsonl.zst",       // already compressed
        "binance_2026-11-04T14.jsonl.zst.tmp",   // unfinished compression
        "binance_2026-11-04T14.jsonl.bak",
        "binance_2026-11-04T25.jsonl",           // no hour 25
        "binance_2026-13-04T14.jsonl",           // no month 13
        "binance_2026-02-30T14.jsonl",           // no 30 Feb
        "binance_2026-02-29T14.jsonl",           // 2026 isn't a leap year
        "binance_2026-11-4T14.jsonl",            // missing digit
        "binance_2026-11-04 14.jsonl",
        "notes.txt",
    };
    bool all_rejected = true;
    for (const char* name : rejected) {
        if (parse_hour_file_name(name, "binance")) {
            std::cout << "        wrongly accepted: " << name << "\n";
            all_rejected = false;
        }
    }
    check(all_rejected, "every name that isn't exactly ours is rejected");
}

// The names compress_hours understands must be exactly the names the
// recorders write - checked against the real HourlyFileSink.
static void test_agrees_with_sink() {
    std::cout << "test_agrees_with_sink\n";
    fs::path base = fresh_dir("sink");
    std::cerr.setstate(std::ios::failbit);  // hide the sink's "writing to" line
    {
        HourlyFileSink sink(base, "binance");
        sink.write({(kH14 + 1234) * 1'000'000, "x\n"});
        sink.flush();
    }
    std::cerr.clear();
    bool ok = false;
    for (const auto& entry : fs::directory_iterator(base / "binance")) {
        ok = parse_hour_file_name(entry.path().filename().string(), "binance") == kH14;
    }
    check(ok, "the file HourlyFileSink wrote parses back to the same hour");
}

static void test_finished_rules() {
    std::cout << "test_finished_rules\n";
    fs::path data = fresh_dir("rules");
    const int64_t long_ago = kH14 - 60 * kMin;

    fs::path t13 = make_file(data / "binance" / "binance_2026-11-04T13.jsonl", long_ago);
    fs::path t14 = make_file(data / "binance" / "binance_2026-11-04T14.jsonl", long_ago);
    fs::path t12_touched = make_file(data / "clob" / "clob_2026-11-04T12.jsonl", kH14 + 5 * kMin);
    make_file(data / "binance" / "notes.txt", long_ago);
    make_file(data / "binance" / "clob_2026-11-04T10.jsonl", long_ago);  // wrong folder
    make_file(data / ".hidden" / ".hidden_2026-11-04T10.jsonl", long_ago);

    std::vector<std::string> errors;
    auto at = [&](int64_t now) { return find_finished(data, now, kDefaultMinAgeSeconds, errors); };

    auto at_1403 = at(kH14 + 3 * kMin);
    check(at_1403.empty(), "at 14:03 nothing is finished yet (13:00 needs until 14:05)");

    auto at_1406 = at(kH14 + 6 * kMin);
    check(at_1406.size() == 1 && at_1406[0].path == t13, "at 14:06 only the 13:00 file is finished");
    // clob 12:00 hour is long over, but was modified at 14:05 - 1 minute
    // before "now" - so it is left alone for now ...
    bool t12_skipped = true;
    for (const auto& f : at_1406) t12_skipped = t12_skipped && f.path != t12_touched;
    check(t12_skipped, "an old hour modified 1 minute ago is skipped");

    // ... and picked up once it has been quiet for 5 minutes, listed
    // before 13:00 because it's the older hour.
    auto at_1411 = at(kH14 + 11 * kMin);
    check(at_1411.size() == 2 && at_1411[0].path == t12_touched && at_1411[1].path == t13,
          "at 14:11 both old hours, oldest first");
    check(errors.empty(), "no errors reading the folders");
    (void)t14;
}

static void test_run_pass() {
    std::cout << "test_run_pass\n";
    fs::path data = fresh_dir("pass");
    const int64_t now = kH14 + 30 * kMin;
    const int64_t long_ago = kH14 - 60 * kMin;

    fs::path a = make_file(data / "binance" / "binance_2026-11-04T12.jsonl", long_ago);
    fs::path b = make_file(data / "polybolt" / "polybolt_2026-11-04T13.jsonl", long_ago);
    fs::path current = make_file(data / "binance" / "binance_2026-11-04T14.jsonl", now);

    // Dry run: lists, touches nothing.
    std::ostringstream log;
    PassResult dry = run_pass(data, now, 3, true, log);
    check(dry.compressed == 2 && dry.failed == 0, "dry run counts 2 files");
    check(log.str().find("would compress binance_2026-11-04T12.jsonl") != std::string::npos, "dry run lists them");
    check(fs::exists(a) && fs::exists(b) && !zst_exists(a) && !zst_exists(b), "dry run changed nothing");

    // Make one file fail: a FOLDER sitting where its .tmp must go can't be
    // overwritten (this fails even when run as root, unlike permissions).
    fs::create_directories(a.string() + ".zst.tmp");

    std::ostringstream log2;
    PassResult real = run_pass(data, now, 3, false, log2);
    std::cout << log2.str();
    check(real.failed == 1 && real.compressed == 1, "one failed, the other was still compressed");
    check(fs::exists(a) && !zst_exists(a), "the failed file's original is kept");
    check(!fs::exists(b) && zst_exists(b), "the other file is now .zst only");
    check(fs::exists(current) && !zst_exists(current), "the current hour was left alone");

    // Problem fixed: the next pass picks the failed file up again.
    fs::remove(a.string() + ".zst.tmp");
    std::ostringstream log3;
    PassResult again = run_pass(data, now, 3, false, log3);
    check(again.failed == 0 && again.compressed == 1 && !fs::exists(a) && zst_exists(a),
          "the next pass compresses it");
}

// A crash after the rename left the original next to its finished .zst.
static void test_finishes_crash_leftover() {
    std::cout << "test_finishes_crash_leftover\n";
    fs::path data = fresh_dir("leftover");
    const int64_t now = kH14 + 30 * kMin;
    fs::path f = make_file(data / "clob" / "clob_2026-11-04T13.jsonl", kH14 - 10 * kMin, "{\"a\":1}\n{\"b\":2}\n");

    std::ostringstream log;
    run_pass(data, now, 3, false, log);
    make_file(f, kH14 - 10 * kMin, "{\"a\":1}\n{\"b\":2}\n");  // the original "comes back"

    std::ostringstream log2;
    PassResult result = run_pass(data, now, 3, false, log2);
    check(result.compressed == 1 && result.failed == 0 && !fs::exists(f) && zst_exists(f),
          "original removed, .zst kept");
}

static void test_lock() {
    std::cout << "test_lock\n";
    fs::path data = fresh_dir("lock");
    std::string error;

    int first = acquire_lock(data, error);
    check(first >= 0, "first copy gets the lock");

    error.clear();
    int second = acquire_lock(data, error);
    check(second < 0, "second copy is refused");
    check(error.find("already running") != std::string::npos, "and is told why: " + error);

    close(first);
    int third = acquire_lock(data, error);
    check(third >= 0, "once the first copy is done, the lock is free again");
    close(third);
}

int main() {
    test_parse_names();
    test_agrees_with_sink();
    test_finished_rules();
    test_run_pass();
    test_finishes_crash_leftover();
    test_lock();

    fs::remove_all(fs::temp_directory_path() / ("compress_hours_test_" + std::to_string(getpid())));

    if (failures == 0) {
        std::cout << "\nall checks passed\n";
        return 0;
    }
    std::cout << "\n" << failures << " check(s) failed\n";
    return 1;
}
