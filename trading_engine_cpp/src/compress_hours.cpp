// compress_hours - one pass over the recorders' data folder: compress
// every finished hourly file to .zst (see compress_pass.h for what
// "finished" means and compress_file.h for how each file is done safely).
//
//     ./build/compress_hours --data-dir data
//
// Does one pass and exits; on the server it is started every 10 minutes,
// at the lowest CPU and disk priority so it never slows the recorders.
// On a Mac the same effect: nice -n 19 ./build/compress_hours --data-dir data
//
// Exit code 0 = everything worked (or there was nothing to do),
// 1 = something failed (details in the output) or bad options.

#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <string_view>
#include <system_error>

#include <unistd.h>  // close

#include <zstd.h>

#include "compress_pass.h"

// 19: the smallest output on a full real hour of clob. Higher levels came
// out no smaller, at twice the time and ten times the memory (benchmark
// in docs/deployment.md, section 5). Decompressing is fast at every level.
constexpr int kDefaultLevel = 19;

static void print_usage(const char* program) {
    std::cerr << "Usage: " << program << " --data-dir <folder> [--level <1-" << ZSTD_maxCLevel()
              << ">] [--dry-run]\n"
              << "  --data-dir  the folder the recorders write to (their --out-dir)\n"
              << "  --level     zstd level (default " << kDefaultLevel << ")\n"
              << "  --dry-run   only list what would be compressed; change nothing\n";
}

int main(int argc, char* argv[]) {
    std::string data_dir;
    int level = kDefaultLevel;
    bool dry_run = false;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        if (arg == "--data-dir" || arg == "--level") {
            if (i + 1 >= argc || argv[i + 1][0] == '\0') {
                std::cerr << arg << " needs a value\n";
                print_usage(argv[0]);
                return 1;
            }
            std::string value = argv[++i];
            if (arg == "--data-dir") {
                data_dir = value;
            } else {
                // Whole value must be a number: "19x" or "abc" is refused,
                // not quietly read as 19 or 0.
                size_t used = 0;
                try {
                    level = std::stoi(value, &used);
                } catch (const std::exception&) {
                    used = 0;
                }
                if (used != value.size() || level < 1 || level > ZSTD_maxCLevel()) {
                    std::cerr << "--level must be a number from 1 to " << ZSTD_maxCLevel() << "\n";
                    return 1;
                }
            }
        } else if (arg == "--dry-run") {
            dry_run = true;
        } else if (arg == "--help" || arg == "-h") {
            print_usage(argv[0]);
            return 0;
        } else {
            std::cerr << "Unknown option: " << arg << "\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    if (data_dir.empty()) {
        std::cerr << "--data-dir is required\n";
        print_usage(argv[0]);
        return 1;
    }
    // Unlike the recorders, never create it: a missing data folder means
    // a wrong path, and there would be nothing to compress anyway.
    std::error_code ec;
    if (!std::filesystem::is_directory(data_dir, ec)) {
        std::cerr << "Not a folder: " << data_dir << "\n";
        return 1;
    }

    std::string error;
    int lock = acquire_lock(data_dir, error);
    if (lock < 0) {
        std::cerr << error << "\n";
        return 1;
    }

    int64_t now = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    PassResult result = run_pass(data_dir, now, level, dry_run, std::cout);

    if (result.compressed == 0 && result.failed == 0) {
        std::cout << "nothing to do\n";
    } else {
        std::cout << result.compressed << (dry_run ? " would be compressed, " : " compressed, ")
                  << result.failed << " failed\n";
    }

    ::close(lock);
    return result.failed > 0 ? 1 : 0;
}
