#include "hourly_file_sink.h"

#include <algorithm>
#include <cerrno>
#include <cstring>   // std::strerror
#include <ctime>     // gmtime_r, strftime
#include <iostream>
#include <system_error>

#include <fcntl.h>     // open
#include <sys/stat.h>  // fstat
#include <unistd.h>    // write, pread, fsync, close

namespace {

constexpr int64_t kMicrosPerHour = 3'600'000'000;

// Past this size the buffer is written out without waiting for flush(),
// so a huge batch (e.g. a backlog after a slow disk) can't pile up in
// memory. 1 MB is a few thousand records - big enough that writes stay
// few and large, small enough not to matter for memory.
constexpr size_t kMaxBuffer = 1 << 20;

}  // namespace

HourlyFileSink::HourlyFileSink(std::filesystem::path base_dir, std::string feed)
    : dir_(std::move(base_dir) / feed), feed_(std::move(feed)) {
    // Say where the data is going - a relative path is measured from the
    // folder the program was started in, which is easy to get wrong.
    std::error_code ec;
    std::filesystem::path shown = std::filesystem::absolute(dir_, ec);
    std::cerr << "--- [" << feed_ << "] writing hourly files to "
              << (ec ? dir_ : shown).string() << " ---" << std::endl;
}

HourlyFileSink::~HourlyFileSink() {
    // The Recorder flushes after every batch, so normally there is nothing
    // left here; this is only a safety net. Losses can't be reported any
    // more at this point, so they are only logged.
    write_buffer();
    if (lost_ > 0) {
        std::cerr << "--- [" << feed_ << "] lost " << lost_
                  << " records while closing ---" << std::endl;
    }
    close_file();
}

bool HourlyFileSink::write(const Record& record) {
    int64_t hour = record.recv_ts / kMicrosPerHour;

    // Files only ever move forward. A record that looks like an earlier
    // hour (the server clock was corrected backwards) goes into the
    // current file rather than reopening an old one.
    if (fd_ < 0 || hour > hour_) {
        if (fd_ >= 0) {
            write_buffer();  // the buffer belongs to the old hour's file
            close_file();
        }
        if (!open_hour(std::max(hour, hour_))) {
            lost_while_failing_++;  // the Recorder counts it from the false
            return false;
        }
    }

    buffer_.append(record.line);
    ends_.push_back(buffer_.size());

    if (buffer_.size() >= kMaxBuffer) {
        write_buffer();
    }
    return true;
}

uint64_t HourlyFileSink::flush() {
    write_buffer();
    uint64_t lost = lost_;
    lost_ = 0;
    return lost;
}

bool HourlyFileSink::open_hour(int64_t hour) {
    std::error_code ec;
    std::filesystem::create_directories(dir_, ec);
    if (ec) {
        report_failure("cannot create " + dir_.string() + ": " + ec.message());
        return false;
    }

    std::filesystem::path path = dir_ / file_name(hour);

    // O_RDWR:   write, and also read - end_with_newline() reads the last byte.
    // O_APPEND: every write goes to the end, so reopening the same hour
    //   after a restart adds to the file instead of overwriting it.
    // O_CREAT:  create the file if it doesn't exist yet (0644 = owner can
    //   write, everyone can read).
    // O_CLOEXEC: don't leak the open file into any program we start.
    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd_ < 0) {
        report_failure("cannot open " + path.string() + ": " + std::strerror(errno));
        return false;
    }

    if (!end_with_newline()) {
        ::close(fd_);
        fd_ = -1;
        return false;
    }

    hour_ = hour;
    return true;
}

bool HourlyFileSink::end_with_newline() {
    // A crash (or a failed write) can leave the file ending in half a
    // record. Appending straight after it would glue our first record onto
    // that half, spoiling both. Adding a newline first keeps the damage to
    // the one broken line, which later processing simply skips.
    struct stat info{};
    if (::fstat(fd_, &info) != 0) {
        report_failure(std::string("cannot check file size: ") + std::strerror(errno));
        return false;
    }
    if (info.st_size == 0) {
        return true;  // new or empty file - nothing to repair
    }

    // pread reads at a given position without moving anything, so it
    // doesn't interfere with O_APPEND writes.
    char last = 0;
    ssize_t n;
    do {
        n = ::pread(fd_, &last, 1, info.st_size - 1);
    } while (n < 0 && errno == EINTR);
    if (n != 1) {
        report_failure(std::string("cannot read last byte: ") + std::strerror(errno));
        return false;
    }
    if (last == '\n') {
        return true;
    }

    do {
        n = ::write(fd_, "\n", 1);
    } while (n < 0 && errno == EINTR);
    if (n != 1) {
        report_failure(std::string("cannot repair half-written line: ") + std::strerror(errno));
        return false;
    }
    // Rare and worth knowing about, so always printed.
    std::cerr << "--- [" << feed_ << "] file ended in a half-written line"
              << " (earlier crash?); added a newline before continuing ---" << std::endl;
    return true;
}

void HourlyFileSink::close_file() {
    if (fd_ < 0) {
        return;
    }
    // fsync: don't return until the data is physically on the disk, not
    // just in the operating system's memory. Slow (milliseconds), which is
    // why it happens once per file, not per batch.
    if (::fsync(fd_) != 0) {
        report_failure(std::string("fsync failed: ") + std::strerror(errno));
    }
    ::close(fd_);
    fd_ = -1;
}

void HourlyFileSink::write_buffer() {
    if (buffer_.empty()) {
        return;
    }

    // write() may take only part of what we give it, so keep going until
    // everything is out or it reports a real error.
    size_t written = 0;
    bool failed = false;
    while (written < buffer_.size()) {
        ssize_t n = ::write(fd_, buffer_.data() + written, buffer_.size() - written);
        if (n < 0) {
            if (errno == EINTR) {
                continue;  // interrupted by a signal (e.g. Ctrl+C) before writing anything - just retry
            }
            report_failure(std::string("write failed: ") + std::strerror(errno));
            failed = true;
            break;
        }
        written += static_cast<size_t>(n);
    }

    if (failed) {
        // Records that ended within the bytes that made it are saved; the
        // rest are lost. ends_ is in order, so count the ones past the cut.
        auto saved_end = std::upper_bound(ends_.begin(), ends_.end(), written);
        uint64_t lost = static_cast<uint64_t>(ends_.end() - saved_end);
        lost_ += lost;
        lost_while_failing_ += lost;

        // The file may now end in half a record. Close it so the next
        // record reopens it, and the reopen adds the repairing newline.
        close_file();
    } else {
        report_success();
    }

    buffer_.clear();
    ends_.clear();
}

void HourlyFileSink::report_failure(const std::string& what) {
    if (failing_) {
        return;  // already reported; stay quiet until it recovers
    }
    failing_ = true;
    std::cerr << "--- [" << feed_ << "] " << what
              << " (records are being dropped; further errors hidden until writing works again) ---"
              << std::endl;
}

void HourlyFileSink::report_success() {
    if (!failing_) {
        return;
    }
    std::cerr << "--- [" << feed_ << "] writing works again; " << lost_while_failing_
              << " records were lost meanwhile ---" << std::endl;
    failing_ = false;
    lost_while_failing_ = 0;
}

std::string HourlyFileSink::file_name(int64_t hour) const {
    // gmtime_r turns seconds since 1970 into a UTC date and time. "_r"
    // is the thread-safe version - plain gmtime shares one result between
    // all threads.
    std::time_t seconds = static_cast<std::time_t>(hour * 3600);
    std::tm utc{};
    gmtime_r(&seconds, &utc);

    char stamp[32];
    std::strftime(stamp, sizeof stamp, "%Y-%m-%dT%H", &utc);
    return feed_ + "_" + stamp + ".jsonl";
}
