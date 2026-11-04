#pragma once

// A sink is where records end up - the end of the pipe the data flows
// through (websocket -> Recorder queue -> writer thread -> sink).
//
// The Recorder moves records; a sink puts them somewhere. Sink is the
// contract every destination must meet, so the Recorder can deliver to any
// of them without knowing which one it has:
//   StreamSink     - any std::ostream (stdout, or a string in tests)
//   HourlyFileSink - one file per UTC hour (coming next)
//
// Only ever called from the Recorder's writer thread, so a sink needs no
// locking of its own.

#include <cstdint>
#include <ostream>
#include <string>

// One queued item: the finished text plus the receive time it was stamped
// with. The timestamp travels separately so a sink can pick the right
// hourly file without parsing the text back apart.
struct Record {
    int64_t recv_ts;   // epoch microseconds, the same number as the record's "recv_ts" field
    std::string line;  // the finished JSONL line, newline included
};

class Sink {
public:
    // virtual destructor: the Recorder deletes a sink through a Sink
    // pointer, and without this the real sink's own destructor (e.g. the
    // one that closes a file) would never run.
    virtual ~Sink() = default;

    // "= 0" means Sink itself has no version of these - every kind of sink
    // must supply its own.
    //
    // write returns false when the record could not be saved (disk full, no
    // permission...). The Recorder counts each false as a dropped record,
    // so a disk problem shows up in the same dropped count as a full queue
    // instead of losing records silently.
    virtual bool write(const Record& record) = 0;  // take one record
    // Push anything buffered out. Returns how many records it failed to
    // save - a sink that buffers only finds out about a disk failure here,
    // after write() already said yes. Counted as dropped, like a false.
    virtual uint64_t flush() = 0;
};

// Today's behaviour: write each line straight to a stream.
class StreamSink : public Sink {
public:
    // Borrows the stream, doesn't own it - std::cout and a test's
    // ostringstream both outlive the Recorder that holds this sink.
    explicit StreamSink(std::ostream& out) : out_(out) {}

    // Always true: a stream can't say which line a failure belongs to, and
    // the only streams used here are stdout and test strings.
    bool write(const Record& record) override {
        out_ << record.line;
        return true;
    }
    uint64_t flush() override {
        out_.flush();
        return 0;
    }

private:
    std::ostream& out_;
};
