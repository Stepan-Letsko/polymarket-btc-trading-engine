#pragma once

// One pass of compress_hours: find every finished hourly file under the
// data folder and compress it with compress_file.
//
// A file is finished when BOTH are true:
//   - the hour in its name ended at least min_age seconds ago, and
//   - it hasn't been modified for at least min_age seconds.
// The second rule is a safety belt: a file something is still writing to
// (e.g. a recorder restarted with its clock behind) is left alone.
//
// Everything takes "now" as an argument instead of reading the clock, so
// the tests can say "pretend it's 14:06" without waiting.

#include <cstdint>
#include <filesystem>
#include <optional>
#include <ostream>
#include <string>
#include <vector>

// 5 minutes, for both rules.
constexpr int64_t kDefaultMinAgeSeconds = 5 * 60;

// "<feed>_YYYY-MM-DDTHH.jsonl" -> the hour's start in seconds since 1970
// (UTC). Anything that doesn't match exactly - another feed's prefix, a
// .zst or .tmp, an impossible date like T25 or Feb 30 - gives nullopt.
std::optional<int64_t> parse_hour_file_name(const std::string& name, const std::string& feed);

struct HourFile {
    std::filesystem::path path;
    int64_t hour_start;  // seconds since 1970, UTC
};

// Every finished hourly file in <data_dir>/<feed>/ folders, oldest hour
// first. Problems reading a folder are added to `errors`.
std::vector<HourFile> find_finished(const std::filesystem::path& data_dir, int64_t now_seconds,
                                    int64_t min_age_seconds, std::vector<std::string>& errors);

struct PassResult {
    int compressed = 0;  // (with dry_run: how many would have been)
    int failed = 0;
};

// Compress everything find_finished returns, logging one line per file.
// A failed file is logged and skipped; the rest still get done.
PassResult run_pass(const std::filesystem::path& data_dir, int64_t now_seconds, int level,
                    bool dry_run, std::ostream& log, int64_t min_age_seconds = kDefaultMinAgeSeconds);

// Makes sure only one compress_hours works on a data folder at a time,
// using a lock on <data_dir>/.compress_hours.lock. Returns the open lock
// file (keep it open for the whole run), or -1 with the reason in `error`
// - including when another copy already holds it. The operating system
// releases the lock automatically when the file is closed or the process
// ends, even by crashing, so it can never be left stuck.
int acquire_lock(const std::filesystem::path& data_dir, std::string& error);
