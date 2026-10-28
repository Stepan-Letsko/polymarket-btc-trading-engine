#include "recorder.h" //including the recorder header file

Recorder::Recorder(std::ostream& out, size_t max_queued) // Making the recorder constructor
    : out_(out), max_queued_(max_queued) { // member list initialiser, i.e. before the constructor runs set the members to given values.
    thread_ = std::thread(&Recorder::run, this); //Creating a thread for the recorder. Run run on this object
}

void Recorder::write(std::string line){ // Initialising recoreder, takes line by copy not reference because then if the reference disapears in other thread we still have a copy to write

    { //Scope block
        std::lock_guard<std::mutex> lock(mutex_); //Takes the mutex token to make thread safe

        if(queue_.size() >= max_queued_){  //Check to see if incoming line exceeds queue size
            dropped_++; //Increment the dropped counter
            return; //RETURN
        }

        queue_.push_back(std::move(line)); //If not overflow push the string to the back of the queue
        //std::move allows us to not copy the line and waste memory
    }

    cv_.notify_one(); //waking up the thread telling it there is work to do
    //must be outsie the closing brace so that mutex is realesed and thread can access queue

}

void Recorder::stop() { // Finish every order still in the queue before shutting down

    {
        std::lock_guard<std::mutex> lock(mutex_); //taking the mutex
        if (stopping_){ //Checking if we are already stopping
            return; //Maked this safe to call multiple times
        }
        stopping_ = true; //Otherwise setting stopping to true
    } //Lock realeased

    cv_.notify_one(); //tells the thread to read stopping to check its state

    if(thread_.joinable()) {  //Check to see if the thread actually exists before we join it
        thread_.join(); // Waits unitl thread is finished its work
    }

}

Recorder::~Recorder() { //Destructor runs automatically when recorder falls out of scope, like at return from main
    stop();  //Safely calls stop to not lose any data
}

uint64_t Recorder::dropped() const { //Return the number of dropped messages
    return dropped_; //No mutex used since this is atomic
}

void Recorder::run() {
    std::vector<std::string> batch; //Seperate vector that is private to the run function

    while (true) { //loop forever
        {
            std::unique_lock<std::mutex> lock(mutex_); //Mutex lock, but unique lock can lock and unlock 
            cv_.wait(lock, [this] { return !queue_.empty() || stopping_; }); //If queue not empty or stopping flag is on then dont sleep and work
            batch.swap(queue_); //swaps the vectors contents
        }

        if (batch.empty()) {
            break;
        }

        for (const std::string& line : batch) {
            out_ << line;
        }
        out_.flush();

        batch.clear();
    }
}
