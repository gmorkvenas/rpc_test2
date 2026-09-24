// promise_client: the std::promise pattern from "C++ Concurrency in Action"
// (listing 4.10), simplified.
//
//   * User threads call connection::send_request(). It creates a
//     std::promise for the reply, stores it under a new id, queues the
//     request and returns the future immediately.
//   * ONE I/O thread runs process_connections(): it sends queued requests
//     and collects the reply frames for each id. When the "END" frame
//     arrives, it calls set_value() on the promise with all collected frames.
//
// Good breakpoints: send_request() and the set_value() call in
// process_connections(). Use Debug > Windows > Threads to see which thread
// sets the promise and which thread is waiting in future.get().

#include "net.h"

#include <atomic>
#include <chrono>
#include <deque>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using reply_t = std::vector<std::string>;   // all frames of one reply, last one is "END"

struct data_packet {                 // a frame received from the server
    uint32_t    id;
    std::string payload;
};

struct outgoing_packet {             // a request waiting to be sent
    uint32_t    id;
    std::string payload;
};

struct pending_request {
    std::promise<reply_t> promise;
    reply_t               frames;    // frames received so far (I/O thread only)
};

class connection {
public:
    explicit connection(socket_t s) : sock_(s) {}
    ~connection() { close_socket(sock_); }
    connection(const connection&) = delete;
    connection& operator=(const connection&) = delete;

    // ---- called from any user thread --------------------------------------
    std::future<reply_t> send_request(const std::string& payload) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_) throw std::runtime_error("connection is closed");

        uint32_t id = next_id_++;
        outgoing_.push_back({id, payload});
        return pending_[id].promise.get_future();
    }

    // ---- called only from the I/O thread ------------------------------------

    // True if a complete frame is available (reads the socket if needed).
    bool has_incoming_data() {
        if (frame_ready()) return true;
        if (closed_ || !wait_readable(sock_, 0)) return false;
        if (recv_some(sock_, read_buffer_) <= 0) {
            close_with_error("connection lost");
            return false;
        }
        return frame_ready();
    }

    data_packet incoming() {
        data_packet packet{};
        try_decode_frame(read_buffer_, packet.id, packet.payload);
        return packet;
    }

    // The request waiting for reply `id`. The reference stays valid while
    // other threads add entries (unordered_map never moves its elements).
    pending_request& get_request(uint32_t id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return pending_.at(id);
    }

    void remove_request(uint32_t id) {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_.erase(id);
    }

    bool has_outgoing_data() {
        std::lock_guard<std::mutex> lock(mutex_);
        return !closed_ && !outgoing_.empty();
    }

    outgoing_packet pop_outgoing() {
        std::lock_guard<std::mutex> lock(mutex_);
        outgoing_packet packet = std::move(outgoing_.front());
        outgoing_.pop_front();
        return packet;
    }

    void send(uint32_t id, const std::string& payload) {
        if (!send_all(sock_, encode_frame(id, payload)))
            close_with_error("send failed");
    }

    bool is_closed() {
        std::lock_guard<std::mutex> lock(mutex_);
        return closed_;
    }

private:
    bool frame_ready() const {
        return read_buffer_.size() >= 8 && read_buffer_.size() >= 8 + get_u32(read_buffer_, 4);
    }

    // Connection broken: every waiting future gets an exception instead of
    // hanging forever.
    void close_with_error(const char* what) {
        std::lock_guard<std::mutex> lock(mutex_);
        closed_ = true;
        auto error = std::make_exception_ptr(std::runtime_error(what));
        for (auto& entry : pending_) entry.second.promise.set_exception(error);
        pending_.clear();
        outgoing_.clear();
    }

    socket_t    sock_;
    std::string read_buffer_;                        // I/O thread only

    std::mutex mutex_;                               // guards everything below
    std::unordered_map<uint32_t, pending_request> pending_;
    std::deque<outgoing_packet> outgoing_;
    uint32_t next_id_ = 1;
    bool     closed_  = false;
};

using connection_set = std::vector<std::unique_ptr<connection>>;

std::atomic<bool> stop_requested{false};

bool done(connection_set& connections) {
    if (stop_requested) return true;
    for (auto& c : connections)
        if (!c->is_closed()) return false;
    return true;                                     // all connections closed
}

// The loop from the book, adapted to multi-frame replies ending with "END".
void process_connections(connection_set& connections) {
    while (!done(connections)) {
        bool did_work = false;
        for (auto& connection : connections) {
            if (connection->has_incoming_data()) {
                data_packet data = connection->incoming();
                pending_request& request = connection->get_request(data.id);
                request.frames.push_back(data.payload);
                if (data.payload == "END") {
                    request.promise.set_value(std::move(request.frames));   // wakes the waiter
                    connection->remove_request(data.id);
                }
                did_work = true;
            }
            if (connection->has_outgoing_data()) {
                outgoing_packet data = connection->pop_outgoing();
                connection->send(data.id, data.payload);
                did_work = true;
            }
        }
        // Not in the book: avoid spinning at 100% CPU when there is nothing to do.
        if (!did_work) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}

socket_t connect_to_server() {
    socket_t s = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(demo_port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        close_socket(s);
        return invalid_socket;
    }
    return s;
}

int main() {
    [[maybe_unused]] net_init net;   // starts Winsock on Windows

    socket_t s = connect_to_server();
    if (s == invalid_socket) {
        std::cerr << "Cannot connect to 127.0.0.1:" << demo_port << " - start promise_server first.\n";
        return 1;
    }

    connection_set connections;
    connections.push_back(std::make_unique<connection>(s));
    connection& conn = *connections.front();

    std::thread io_thread(process_connections, std::ref(connections));

    const std::vector<std::string> messages = {"hello", "promise", "world"};

    std::vector<std::future<reply_t>> replies;
    for (const auto& msg : messages) {
        replies.push_back(conn.send_request(msg));
        std::cout << "sent '" << msg << "'\n";
    }

    for (std::size_t i = 0; i < replies.size(); ++i) {
        try {
            reply_t frames = replies[i].get();       // blocks until "END" arrives
            std::cout << "reply for '" << messages[i] << "':";
            for (const auto& f : frames) std::cout << " '" << f << "'";
            std::cout << '\n';
        } catch (const std::exception& e) {
            std::cout << "request '" << messages[i] << "' failed: " << e.what() << '\n';
        }
    }

    stop_requested = true;
    io_thread.join();
    std::cout << "done\n";
}
