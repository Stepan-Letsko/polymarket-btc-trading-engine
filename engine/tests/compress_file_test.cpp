// Tests for compress_file. Works in a throwaway folder under the system
// temp directory, and checks results by decompressing with zstd directly -
// independently of the code being tested.
//
// Build and run:
//   cd build && cmake .. && cmake --build . && ./compress_file_test
// Exits 0 if every check passed, 1 otherwise.

#include "compression/compress_file.h"

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include <unistd.h>  // getpid

// Unlocks ZSTD_getFrameHeader, used to confirm the checksum is switched on.
// Allowed because we link zstd statically, so its version can't change
// underneath us.
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

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

static std::string read_file(const fs::path& path) {
    std::ifstream in(path, std::ios::binary);
    std::ostringstream text;
    text << in.rdbuf();
    return text.str();
}

static void write_file(const fs::path& path, const std::string& content) {
    std::ofstream(path, std::ios::binary) << content;
}

static fs::path fresh_dir(const std::string& name) {
    fs::path dir = fs::temp_directory_path() /
                   ("compress_file_test_" + std::to_string(getpid())) / name;
    fs::remove_all(dir);
    fs::create_directories(dir);
    return dir;
}

// Decompress a whole .zst with zstd's one-shot function. Uses the content
// size compress_file stores in the file; returns false if anything is off.
static bool unzstd(const fs::path& path, std::string& out) {
    std::string packed = read_file(path);
    unsigned long long size = ZSTD_getFrameContentSize(packed.data(), packed.size());
    if (size == ZSTD_CONTENTSIZE_ERROR || size == ZSTD_CONTENTSIZE_UNKNOWN) {
        return false;
    }
    out.assign(static_cast<size_t>(size), '\0');
    size_t got = ZSTD_decompress(out.data(), out.size(), packed.data(), packed.size());
    return !ZSTD_isError(got) && got == size;
}

static bool has_checksum(const fs::path& path) {
    std::string packed = read_file(path);
    ZSTD_frameHeader header{};
    return ZSTD_getFrameHeader(&header, packed.data(), packed.size()) == 0 && header.checksumFlag == 1;
}

// Lines that look like our records, varied enough that zstd can't shrink
// them to nothing (a simple pseudo-random number generator, fixed seed).
static std::string sample_records(size_t count) {
    std::string text;
    uint64_t x = 42;
    for (size_t i = 0; i < count; ++i) {
        x = x * 6364136223846793005ULL + 1442695040888963407ULL;
        text += "{\"recv_ts\":" + std::to_string(1793800800000000ULL + i * 997) +
                ",\"data\":{\"price\":\"" + std::to_string(78000 + (x >> 52)) +
                "." + std::to_string((x >> 20) % 100) + "\",\"u\":" + std::to_string(x >> 30) + "}}\n";
    }
    return text;
}

// Shared checks for a successful run: only the .zst is left, and it holds
// exactly the original bytes.
static void check_done(const fs::path& source, const std::string& original) {
    fs::path zst = source;
    zst += ".zst";
    fs::path tmp = zst;
    tmp += ".tmp";
    std::string restored;
    check(!fs::exists(source), "original deleted");
    check(!fs::exists(tmp), "no .tmp left behind");
    check(fs::exists(zst) && unzstd(zst, restored) && restored == original,
          ".zst decompresses to exactly the original bytes");
}

static void test_normal() {
    std::cout << "test_normal\n";
    fs::path source = fresh_dir("normal") / "feed_2026-11-04T14.jsonl";
    std::string original = sample_records(1000);
    write_file(source, original);

    std::string error;
    check(compress_file(source, 3, error), "compress_file succeeded");
    check_done(source, original);

    fs::path zst = source;
    zst += ".zst";
    check(has_checksum(zst), "checksum is stored in the .zst");
    std::cout << "        " << original.size() << " bytes -> " << fs::file_size(zst) << " bytes\n";
}

// Several MB: many 128 KB pieces in both directions, so the piece-by-piece
// loops are exercised, not just a single pass.
static void test_large() {
    std::cout << "test_large\n";
    fs::path source = fresh_dir("large") / "feed.jsonl";
    std::string original = sample_records(60000);
    write_file(source, original);

    std::string error;
    check(compress_file(source, 3, error), "compress_file succeeded on " + std::to_string(original.size() / 1024) + " KB");
    check_done(source, original);
}

static void test_empty() {
    std::cout << "test_empty\n";
    fs::path source = fresh_dir("empty") / "feed.jsonl";
    write_file(source, "");

    std::string error;
    check(compress_file(source, 3, error), "compress_file succeeded on an empty file");
    check_done(source, "");
}

// An earlier run crashed mid-compression, leaving a broken .tmp.
static void test_stale_tmp() {
    std::cout << "test_stale_tmp\n";
    fs::path source = fresh_dir("stale_tmp") / "feed.jsonl";
    std::string original = sample_records(500);
    write_file(source, original);
    write_file(source.string() + ".zst.tmp", "half-written garbage from a crash");

    std::string error;
    check(compress_file(source, 3, error), "compress_file succeeded despite the stale .tmp");
    check_done(source, original);
}

// An earlier run got as far as the rename, then crashed before deleting
// the original: both a good .zst and the original exist.
static void test_crash_after_rename() {
    std::cout << "test_crash_after_rename\n";
    fs::path source = fresh_dir("after_rename") / "feed.jsonl";
    std::string original = sample_records(500);
    write_file(source, original);

    std::string error;
    compress_file(source, 3, error);
    write_file(source, original);  // bring the original back, as if never deleted

    check(compress_file(source, 3, error), "compress_file finished the job");
    check_done(source, original);
}

// A .zst that holds DIFFERENT data from the original. Should never happen;
// if it does, nothing may be deleted.
static void test_mismatched_zst() {
    std::cout << "test_mismatched_zst\n";
    fs::path source = fresh_dir("mismatch") / "feed.jsonl";
    write_file(source, sample_records(500));
    std::string error;
    compress_file(source, 3, error);

    std::string different = sample_records(501);  // one extra line
    write_file(source, different);
    std::string zst_before = read_file(source.string() + ".zst");

    error.clear();
    check(!compress_file(source, 3, error), "compress_file refused");
    check(error.find("does not match") != std::string::npos, "error says they don't match: " + error);
    check(read_file(source) == different, "original left untouched");
    check(read_file(source.string() + ".zst") == zst_before, ".zst left untouched");
}

static void test_missing_source() {
    std::cout << "test_missing_source\n";
    fs::path source = fresh_dir("missing") / "nope.jsonl";
    std::string error;
    check(!compress_file(source, 3, error), "compress_file returns false");
    check(!error.empty(), "and says why: " + error);
}

// A bad level must fail before anything is written, keeping the original.
static void test_bad_level() {
    std::cout << "test_bad_level\n";
    fs::path source = fresh_dir("bad_level") / "feed.jsonl";
    std::string original = sample_records(10);
    write_file(source, original);

    std::string error;
    check(!compress_file(source, 1000, error), "level 1000 is refused");
    check(read_file(source) == original, "original left untouched");
    check(!fs::exists(source.string() + ".zst") && !fs::exists(source.string() + ".zst.tmp"),
          "no .zst or .tmp created");
}

int main() {
    test_normal();
    test_large();
    test_empty();
    test_stale_tmp();
    test_crash_after_rename();
    test_mismatched_zst();
    test_missing_source();
    test_bad_level();

    fs::remove_all(fs::temp_directory_path() / ("compress_file_test_" + std::to_string(getpid())));

    if (failures == 0) {
        std::cout << "\nall checks passed\n";
        return 0;
    }
    std::cout << "\n" << failures << " check(s) failed\n";
    return 1;
}
