#pragma once

// A sink that writes one file per UTC hour:
//     <base_dir>/<feed>/<feed>_2026-11-04T14.jsonl
//
// The hour comes from each record's own recv_ts, not from the clock when
// the writer thread gets to it, so a record received at 13:59:59.999 lands
// in the 13:00 file even if it is written a moment after 14:00.
//
// Uses the operating system's own file calls (open / write / fsync /
// close) rather than a C++ stream, for two reasons:
//   - we hold the buffer ourselves, so after a failed write we know
//     exactly which records did not make it, and can count them;
//   - fsync forces a finished hour physically onto the disk, so a power
//     cut can't lose it. A C++ stream has no way to do that.

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>
#include "recording/sink.h"

class HourlyFileSink : public Sink {
public:
    HourlyFileSink(std::filesystem::path base_dir, std::string feed);
    ~HourlyFileSink() override; // writes what's left, fsyncs and closes

    // For startup: makes sure <base_dir>/<feed> exists (creating it if
    // needed) and that we're allowed to write in it. Prints the reason and
    // returns false if not, so a mistyped --out-dir stops the program
    // straight away instead of it running for hours dropping everything.
    static bool check_folder(const std::filesystem::path& base_dir, const std::string& feed);

    // Owns an open file, so copying makes no sense - two copies would both
    // try to close the same file.
    HourlyFileSink(const HourlyFileSink&) = delete;
    HourlyFileSink& operator=(const HourlyFileSink&) = delete;

    bool write(const Record& record) override; // adds to the buffer; opens a new file when the hour changes
    uint64_t flush() override;                 // sends the buffer to the file

private:
    bool open_hour(int64_t hour);  // open (or create) the file for this hour
    bool end_with_newline();       // repair a half-written last line left by a crash
    void close_file();             // fsync then close the current file
    void write_buffer();           // send the buffer to the file, counting any losses
    std::string file_name(int64_t hour) const;

    // Error logging that can't flood: a full disk fails every write, many
    // times a second, so only the first failure is printed, then one line
    // when writing works again, with how many records were lost meanwhile.
    void report_failure(const std::string& what);
    void report_success();

    std::filesystem::path dir_;    // <base_dir>/<feed>
    std::string feed_;

    int fd_ = -1;                  // the open file, -1 = none open
    int64_t hour_ = -1;            // hours since 1970 of the open file, -1 = none yet

    // Records waiting to be written. ends_ holds where each record ends in
    // buffer_, so after a write that stopped part way we can tell which
    // records got onto disk in full and which didn't.
    std::string buffer_;
    std::vector<size_t> ends_;

    uint64_t lost_ = 0;            // failed records not yet reported by flush()

    bool failing_ = false;         // true from the first failure until a write succeeds again
    uint64_t lost_while_failing_ = 0; // records lost since failing_ became true, for the recovery line
};
