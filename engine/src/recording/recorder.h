#pragma once //compile once

#include <atomic> //allows for a counter that two threads can safely handle
#include <condition_variable> // Used to wake up a thread when there is work so that it does not idle and waste cpu
#include <cstdint> //Used to get distict bit numbers eg uint64_t is 64 bytes not matter what os
#include <memory> // std::unique_ptr, so the Recorder owns its sink
#include <mutex> // brings std::mutex used to lock/unlock variable to prevent race condition
#include <string>
#include <thread>
#include <vector>
#include "recording/sink.h" // Record, and the Sink the writer thread delivers to

class Recorder { //New recorder class
public:
    explicit Recorder(std::unique_ptr<Sink> sink, size_t max_queued = 100000); //Constructor
    //sink is where records end up; the Recorder owns it, so the sink lives
    //exactly as long as the writer thread that uses it
    //max_queued is the queue size
    ~Recorder(); //The desturctor

    void write(int64_t recv_ts, std::string line);   // safe from any thread, never does I/O //lets another part of the code give the recorder a line to write
    uint64_t dropped() const;       // lines discarded under overload // check to see what we dropped
    void stop();                    // drain the queue, stop the thread

private:
    void run();                     // the writer thread's loop 

    std::unique_ptr<Sink> sink_; // Where we write. Declared before thread_, so it is created before the thread starts and destroyed after it has stopped
    size_t max_queued_; // THe max size of the queue

    std::mutex mutex_; // Token for what thread get to use certain variables
    std::condition_variable cv_; //The wake up bell to wake a thread up
    std::vector<Record> queue_; // vector to hold the records that we will write
    bool stopping_ = false; //flag to tell teh recorded if it needs to stop or not

    std::atomic<uint64_t> dropped_{0}; //atomic counter for thread safely counting how many messages we have dropped
    std::thread thread_; //Declaring the thread (worker)
};
