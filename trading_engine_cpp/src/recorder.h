#pragma once //compile once

#include <atomic> //allows for a counter that two threads can safely handle
#include <condition_variable> // Used to wake up a thread when there is work so that it does not idle and waste cpu
#include <cstdint> //Used to get distict bit numbers eg uint64_t is 64 bytes not matter what os
#include <mutex> // brings std::mutex used to lock/unlock variable to prevent race condition
#include <ostream> //std::ostream for controling where to write the output to
#include <string> 
#include <thread>
#include <vector>

class Recorder { //New recorder class
public:
    explicit Recorder(std::ostream& out, size_t max_queued = 100000); //Constructor
    //Out tells us where to write to
    //max_queued is the queue size
    ~Recorder(); //The desturctor

    void write(std::string line);   // safe from any thread, never does I/O //lets another part of the code give the recorder a line to write
    uint64_t dropped() const;       // lines discarded under overload // check to see what we dropped
    void stop();                    // drain the queue, stop the thread

private:
    void run();                     // the writer thread's loop 

    std::ostream& out_; // The location where we write 
    size_t max_queued_; // THe max size of the queue

    std::mutex mutex_; // Token for what thread get to use certain variables
    std::condition_variable cv_; //The wake up bell to wake a thread up
    std::vector<std::string> queue_; // vector to hold strings that we will write
    bool stopping_ = false; //flag to tell teh recorded if it needs to stop or not

    std::atomic<uint64_t> dropped_{0}; //atomic counter for thread safely counting how many messages we have dropped
    std::thread thread_; //Declaring the thread (worker)
};
