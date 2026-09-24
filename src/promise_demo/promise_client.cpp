// promise_client: the std::promise pattern from "C++ Concurrency in Action"
// (listing 4.10), implemented for real.
//
//   * User threads call connection::send_request(). It creates
//       - a std::promise<std::string> for the reply, stored under a new id
//       - an outgoing packet holding a std::promise<bool> ("was it sent?")
//     and returns the futures immediately.
//   * ONE I/O thread runs process_connections(): it sends queued packets
//     (fulfilling the "sent" promise) and, for every reply that arrives,
//     looks up the promise by the reply's id and calls set_value().
//
// Good breakpoints: send_request(), and the two set_value() calls in
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

struct data_packet {                 // a reply received from the server
    uint32_t    id;
    std::string payload;
};

struct outgoing_packet {             // a request waiting to be sent
    uint32_t           id;
    std::string        payload;
    std::promise<bool> promise;      // fulfilled once the bytes are sent
};

struct request_handle {              // what the caller gets back
    uint32_t                 id;
    std::future<bool>        sent;
    std::future<std::string> reply;
};

class connection {
public:
    explicit connection(socket_t s) : sock_(s) {}
    ~connection() { close_socket(sock_); }
    connection(const connection&) = delete;
    connection& operator=(const connection&) = delete;

    // ---- called from any user thread --------------------------------------
    request_handle send_request(const std::string& payload) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_) throw std::runtime_error("connection is closed");

        uint32_t id = next_id_++;
        request_handle handle;
        handle.id    = id;
        handle.reply = pending_[id].get_future();          // promise for the reply

        outgoing_packet packet{id, payload, std::promise<bool>()};
        handle.sent = packet.promise.get_future();         // promise for "sent"
        outgoing_.push_back(std::move(packet));
        return handle;
    }

    // ---- called only from the I/O thread ------------------------------------

    // True if a complete reply frame is available (reads the socket if needed).
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

    // The promise waiting for reply `id`. The reference stays valid while
    // other threads add entries (unordered_map never moves its elements).
    std::promise<std::string>& get_promise(uint32_t id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return pending_.at(id);
    }

    void remove_promise(uint32_t id) {
        std::lock_guard<std::mutex> lock(mutex_);
        pending_.erase(id);
    }

    bool has_outgoing_data() {
        std::lock_guard<std::mutex> lock(mutex_);
        return !closed_ && !outgoing_.empty();
    }

    // Takes the first queued packet. (The book calls this top_of_outgoing_queue;
    // it must also remove it, because std::promise can only be moved, not copied.)
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
    // hanging forever (otherwise get() would block, or throw broken_promise).
    void close_with_error(const char* what) {
        std::lock_guard<std::mutex> lock(mutex_);
        closed_ = true;
        auto error = std::make_exception_ptr(std::runtime_error(what));
        for (auto& entry : pending_) entry.second.set_exception(error);
        for (auto& packet : outgoing_) packet.promise.set_exception(error);
        pending_.clear();
        outgoing_.clear();
    }

    socket_t    sock_;
    std::string read_buffer_;                        // I/O thread only

    std::mutex mutex_;                               // guards everything below
    std::unordered_map<uint32_t, std::promise<std::string>> pending_;
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

// The loop from the book, almost line by line.
void process_connections(connection_set& connections) {
    while (!done(connections)) {
        bool did_work = false;
        for (auto& connection : connections) {
            if (connection->has_incoming_data()) {
                data_packet data = connection->incoming();
                std::promise<std::string>& p = connection->get_promise(data.id);
                p.set_value(data.payload);           // wakes whoever waits on reply
                connection->remove_promise(data.id);
                did_work = true;
            }
            if (connection->has_outgoing_data()) {
                outgoing_packet data = connection->pop_outgoing();
                connection->send(data.id, data.payload);
                data.promise.set_value(true);        // wakes whoever waits on "sent"
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

    // Send several requests at once. Replies to "delay" requests come back
    // later, so they arrive in a different order than they were sent.
    const std::vector<std::string> commands = {
        "delay:900 slow reply",
        "upper:hello promise",
        "add:2,40",
        "delay:300 medium reply",
        "unknown command",
    };

    auto start = std::chrono::steady_clock::now();
    std::vector<request_handle> requests;
    for (const auto& cmd : commands) {
        requests.push_back(conn.send_request(cmd));
        std::cout << "queued  #" << requests.back().id << " '" << cmd << "'\n";
    }

    // The "sent" futures complete as soon as the I/O thread has written each request.
    for (auto& r : requests) r.sent.get();
    std::cout << "all requests sent\n\n";

    // Print replies in the order they ARRIVE (poll all futures, like wait_any).
    std::size_t remaining = requests.size();
    while (remaining > 0) {
        for (std::size_t i = 0; i < requests.size(); ++i) {
            auto& r = requests[i];
            if (r.reply.valid() &&
                r.reply.wait_for(std::chrono::milliseconds(1)) == std::future_status::ready) {
                auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - start).count();
                try {
                    std::string value = r.reply.get();   // rethrows if set_exception was used
                    std::cout << "reply   #" << r.id << " after " << ms << " ms: '"
                              << value << "'  (for '" << commands[i] << "')\n";
                } catch (const std::exception& e) {
                    std::cout << "request #" << r.id << " failed after " << ms << " ms: "
                              << e.what() << '\n';
                }
                --remaining;
            }
        }
    }

    stop_requested = true;
    io_thread.join();
    std::cout << "done\n";
}
