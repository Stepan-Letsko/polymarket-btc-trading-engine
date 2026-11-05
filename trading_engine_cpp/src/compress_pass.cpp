#include "compress_pass.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>    // std::snprintf
#include <cstring>   // std::strerror
#include <system_error>

#include <fcntl.h>     // open
#include <sys/file.h>  // flock
#include <sys/stat.h>  // stat
#include <unistd.h>    // close

#include "compress_file.h"

namespace fs = std::filesystem;

namespace {

bool is_digits(const std::string& s, size_t from, size_t count) {
    for (size_t i = from; i < from + count; ++i) {
        if (s[i] < '0' || s[i] > '9') {
            return false;
        }
    }
    return true;
}

int to_int(const std::string& s, size_t from, size_t count) {
    int value = 0;
    for (size_t i = from; i < from + count; ++i) {
        value = value * 10 + (s[i] - '0');
    }
    return value;
}

bool is_leap(int year) {
    return (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
}

int days_in_month(int year, int month) {
    static const int days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    return month == 2 && is_leap(year) ? 29 : days[month - 1];
}

// Days from 1970-01-01 to the given date - Howard Hinnant's well-known
// exact algorithm. Pure arithmetic, so unlike the C library's time
// functions it can't be affected by the computer's time zone setting.
int64_t days_from_civil(int y, int m, int d) {
    y -= m <= 2;
    const int era = (y >= 0 ? y : y - 399) / 400;
    const int yoe = y - era * 400;
    const int doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
    const int doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return static_cast<int64_t>(era) * 146097 + doe - 719468;
}

std::string megabytes(uintmax_t bytes) {
    char text[32];
    std::snprintf(text, sizeof text, "%.2f MB", static_cast<double>(bytes) / 1e6);
    return text;
}

}  // namespace

std::optional<int64_t> parse_hour_file_name(const std::string& name, const std::string& feed) {
    // "<feed>_" + "YYYY-MM-DDTHH" + ".jsonl"
    const std::string prefix = feed + "_";
    const std::string suffix = ".jsonl";
    const size_t stamp_len = 13;
    if (name.size() != prefix.size() + stamp_len + suffix.size() ||
        name.compare(0, prefix.size(), prefix) != 0 ||
        name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0) {
        return std::nullopt;
    }

    const size_t p = prefix.size();  // where YYYY starts
    if (!is_digits(name, p, 4) || name[p + 4] != '-' ||
        !is_digits(name, p + 5, 2) || name[p + 7] != '-' ||
        !is_digits(name, p + 8, 2) || name[p + 10] != 'T' ||
        !is_digits(name, p + 11, 2)) {
        return std::nullopt;
    }

    int year = to_int(name, p, 4);
    int month = to_int(name, p + 5, 2);
    int day = to_int(name, p + 8, 2);
    int hour = to_int(name, p + 11, 2);
    if (month < 1 || month > 12 || day < 1 || day > days_in_month(year, month) || hour > 23) {
        return std::nullopt;
    }
    return days_from_civil(year, month, day) * 86400 + hour * 3600;
}

std::vector<HourFile> find_finished(const fs::path& data_dir, int64_t now_seconds,
                                    int64_t min_age_seconds, std::vector<std::string>& errors) {
    std::vector<HourFile> found;
    std::error_code ec;

    // Each feed is a folder: data/binance, data/clob, data/polybolt.
    // Folders starting with "." are skipped (hidden, not ours).
    for (fs::directory_iterator feeds(data_dir, ec), end; !ec && feeds != end; feeds.increment(ec)) {
        if (!feeds->is_directory(ec) || feeds->path().filename().string()[0] == '.') {
            continue;
        }
        const std::string feed = feeds->path().filename().string();

        std::error_code ec2;
        for (fs::directory_iterator files(feeds->path(), ec2), end2; !ec2 && files != end2; files.increment(ec2)) {
            if (!files->is_regular_file(ec2)) {
                continue;
            }
            std::optional<int64_t> hour_start = parse_hour_file_name(files->path().filename().string(), feed);
            if (!hour_start) {
                continue;  // not a name we fully understand - never touch it
            }

            // Rule 1: the hour ended at least min_age ago.
            if (now_seconds < *hour_start + 3600 + min_age_seconds) {
                continue;
            }

            // Rule 2: not modified for at least min_age. stat's st_mtime
            // is plain seconds since 1970, directly comparable with now.
            struct stat info{};
            if (::stat(files->path().c_str(), &info) != 0) {
                errors.push_back("cannot check " + files->path().string() + ": " + std::strerror(errno));
                continue;
            }
            if (now_seconds - static_cast<int64_t>(info.st_mtime) < min_age_seconds) {
                continue;
            }

            found.push_back({files->path(), *hour_start});
        }
        if (ec2) {
            errors.push_back("cannot read folder " + feeds->path().string() + ": " + ec2.message());
        }
    }
    if (ec) {
        errors.push_back("cannot read folder " + data_dir.string() + ": " + ec.message());
    }

    // Oldest hour first; same hour (different feeds) in name order, so
    // the order is always the same from run to run.
    std::sort(found.begin(), found.end(), [](const HourFile& a, const HourFile& b) {
        return a.hour_start != b.hour_start ? a.hour_start < b.hour_start : a.path < b.path;
    });
    return found;
}

PassResult run_pass(const fs::path& data_dir, int64_t now_seconds, int level,
                    bool dry_run, std::ostream& log, int64_t min_age_seconds) {
    PassResult result;
    std::vector<std::string> errors;
    std::vector<HourFile> files = find_finished(data_dir, now_seconds, min_age_seconds, errors);

    for (const std::string& error : errors) {
        log << "FAILED " << error << "\n";
        result.failed++;
    }

    for (const HourFile& file : files) {
        std::error_code ec;
        uintmax_t before = fs::file_size(file.path, ec);
        const std::string name = file.path.filename().string();

        if (dry_run) {
            log << "would compress " << name << " (" << megabytes(before) << ")\n";
            result.compressed++;
            continue;
        }

        auto started = std::chrono::steady_clock::now();
        std::string error;
        if (!compress_file(file.path, level, error)) {
            log << "FAILED " << name << ": " << error << "\n";
            result.failed++;
            continue;
        }
        double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();

        fs::path zst = file.path;
        zst += ".zst";
        uintmax_t after = fs::file_size(zst, ec);
        char line[160];
        std::snprintf(line, sizeof line, ": %s -> %s (%.1fx) in %.1fs",
                      megabytes(before).c_str(), megabytes(after).c_str(),
                      after > 0 ? static_cast<double>(before) / static_cast<double>(after) : 0.0, seconds);
        log << "compressed " << name << line << "\n";
        result.compressed++;
    }
    return result;
}

int acquire_lock(const fs::path& data_dir, std::string& error) {
    fs::path lock_path = data_dir / ".compress_hours.lock";
    int fd = ::open(lock_path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) {
        error = "cannot open " + lock_path.string() + ": " + std::strerror(errno);
        return -1;
    }
    // LOCK_EX: exclusive, only one holder. LOCK_NB: don't wait for it -
    // if another copy has it, fail straight away.
    if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
        error = errno == EWOULDBLOCK
                    ? "another compress_hours is already running on " + data_dir.string()
                    : "cannot lock " + lock_path.string() + ": " + std::strerror(errno);
        ::close(fd);
        return -1;
    }
    return fd;
}
