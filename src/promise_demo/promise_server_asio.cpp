// promise_server_asio: the same echo server as promise_server.cpp, but built
// on Asio instead of select().
//
// Why this scales better:
//   - No set of sockets is rebuilt and scanned every round. The OS (IOCP on
//     Windows, epoll on Linux) reports only the operations that completed.
//   - Several threads call io_service::run(), so work is spread over all cores.
//   - Each connection is a `session` object that keeps its own state and
//     lives as long as it has a pending async operation (shared_ptr).
//
// Protocol (unchanged, so promise_client works as is): every request frame
// carries an id. For each request the server sends two reply frames with
// the same id:
//   1. the request text (echo)
//   2. "END"                    -> tells the client the reply is complete
//
// The Asio used here is the copy bundled with rpclib (version 1.10.6). It
// lives in namespace clmdep_asio and uses the older io_service name
// (newer Asio versions call it io_context).

#include <asio.hpp>

#include "net.h"   // only for encode_frame / try_decode_frame / demo_port

#include <algorithm>
#include <array>
#include <csignal>
#include <deque>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace asio = clmdep_asio;
using asio::ip::tcp;

// Several threads write to std::cout; the mutex keeps lines from mixing.
std::mutex log_mutex;

template <class... Args>
void log(const Args&... args) {
    std::lock_guard<std::mutex> lock(log_mutex);
    (std::cout << ... << args) << '\n';
}

std::string to_string(const tcp::endpoint& e) {
    std::ostringstream out;
    out << e;   // "127.0.0.1:9090"
    return out.str();
}

std::string thread_name() {
    std::ostringstream out;
    out << "thread " << std::this_thread::get_id();
    return out.str();
}

// One client connection.
//
// The session keeps itself alive: every async call captures `self`
// (a shared_ptr to this object). When the last pending operation finishes
// without starting a new one, the session is destroyed and its socket closed.
//
// All handlers of one session go through `strand_`, so even with many
// threads running the io_service, two handlers of the same session never
// run at the same time. That is why read_buffer_ and write_queue_ need no
// mutex. Different sessions still run in parallel.
class session : public std::enable_shared_from_this<session> {
public:
    session(tcp::socket socket, std::string description)
        : socket_(std::move(socket)),
          strand_(socket_.get_io_service()),
          description_(std::move(description)) {}

    ~session() { log("client disconnected: ", description_); }

    void start() { do_read(); }

private:
    // Ask the OS to fill chunk_ with whatever arrives. The call returns
    // immediately; the lambda runs later, when data is there.
    void do_read() {
        auto self = shared_from_this();
        socket_.async_read_some(
            asio::buffer(chunk_),
            strand_.wrap([this, self](const std::error_code& ec, std::size_t n) {
                if (ec) {
                    // eof = the client closed the connection normally
                    if (ec != asio::error::eof)
                        log("read error on ", description_, ": ", ec.message());
                    return;   // no new read -> session ends once writes finish
                }
                read_buffer_.append(chunk_.data(), n);

                uint32_t    id;
                std::string request;
                while (try_decode_frame(read_buffer_, id, request)) {
                    log("  request #", id, " '", request, "'  (", thread_name(), ")");
                    queue_write(encode_frame(id, request) + encode_frame(id, "END"));
                }
                do_read();   // wait for the next data
            }));
    }

    // Only one async_write may be in flight per socket, otherwise the bytes
    // of two replies could interleave. Replies wait in write_queue_ and are
    // sent one after another.
    void queue_write(std::string data) {
        bool idle = write_queue_.empty();
        write_queue_.push_back(std::move(data));
        if (idle) do_write();
    }

    void do_write() {
        auto self = shared_from_this();
        // async_write (unlike async_write_some) completes only when the
        // whole buffer is sent. The buffer must stay valid until then, which
        // it does because it stays at the front of write_queue_.
        asio::async_write(
            socket_, asio::buffer(write_queue_.front()),
            strand_.wrap([this, self](const std::error_code& ec, std::size_t) {
                if (ec) {
                    log("write error on ", description_, ": ", ec.message());
                    std::error_code ignored;
                    socket_.close(ignored);   // makes the pending read fail too
                    return;
                }
                write_queue_.pop_front();
                if (!write_queue_.empty()) do_write();
            }));
    }

    tcp::socket                socket_;
    asio::io_service::strand   strand_;
    std::string                description_;   // "127.0.0.1:9090 <-> 127.0.0.1:54012  (client 1)"
    std::array<char, 4096>     chunk_;         // target of the current async_read_some
    std::string                read_buffer_;   // bytes received but not yet parsed into frames
    std::deque<std::string>    write_queue_;   // front() is being sent right now
};

// Accepts connections and creates a session for each one.
class server {
public:
    server(asio::io_service& io, uint16_t port)
        // acceptor_ is the listening socket, like `listener` in promise_server.cpp.
        // Its constructor does in one step what the select() version does by hand:
        //   ::socket()                   create the socket
        //   setsockopt(SO_REUSEADDR)     allow a quick restart on the same port
        //   ::bind()                     to the endpoint below
        //   ::listen(SOMAXCONN)          start accepting connections
        // If any step fails (e.g. port already in use) it throws
        // std::system_error, which main() catches.
        //   tcp::endpoint(address, port) = the sockaddr_in of the select() version
        //   address_v4::loopback()       = 127.0.0.1, so only local clients can connect
        //                                  (address_v4::any() would be 0.0.0.0 = all interfaces)
        : acceptor_(io, tcp::endpoint(asio::ip::address_v4::loopback(), port)),
          // socket_ is an empty socket, not connected to anything yet.
          // async_accept() below turns it into the connection with the next
          // client (the select() version gets that socket back from ::accept()).
          // Both objects take `io`: their async operations report completion
          // through this io_service, i.e. on the threads that call io.run().
          socket_(io) {
        // Start the first async accept. This only registers the operation and
        // returns immediately; nothing is accepted until io.run() is called in main().
        do_accept();
    }

private:
    void do_accept() {
        // The next connection is accepted into socket_. Only one accept is
        // pending at a time, so this handler never runs on two threads at
        // once and client_count_ needs no lock.
        acceptor_.async_accept(socket_, [this](const std::error_code& ec) {
            if (!ec) {
                // Small request/reply messages: send them right away instead
                // of waiting to combine them with later data (Nagle).
                socket_.set_option(tcp::no_delay(true));

                std::string description = to_string(socket_.local_endpoint()) + " <-> " +
                                          to_string(socket_.remote_endpoint()) + "  (client " +
                                          std::to_string(++client_count_) + ')';
                log("client connected: ", description);
                std::make_shared<session>(std::move(socket_), std::move(description))->start();
            } else {
                log("accept error: ", ec.message());
            }
            do_accept();   // a moved-from socket can be used for the next accept
        });
    }

    tcp::acceptor acceptor_;
    tcp::socket   socket_;
    int           client_count_ = 0;
};

int main() {
    std::cout << std::unitbuf;   // flush every output, so logs show up even when redirected to a file

    asio::io_service io;

    try {
        server srv(io, demo_port);

        // Ctrl+C stops the io_service, so all run() calls return and main exits.
        asio::signal_set signals(io, SIGINT, SIGTERM);
        signals.async_wait([&io](const std::error_code&, int) {
            log("stopping...");
            io.stop();
        });

        unsigned thread_count = std::max(1u, std::thread::hardware_concurrency());
        log("promise_server_asio listening on 127.0.0.1:", demo_port, " with ", thread_count,
            " threads");

        // Every thread that calls run() takes completed operations from the
        // OS and runs their handlers. The main thread is one of them.
        std::vector<std::thread> pool;
        for (unsigned i = 1; i < thread_count; ++i)
            pool.emplace_back([&io] { io.run(); });
        io.run();

        for (auto& t : pool) t.join();
    } catch (const std::exception& e) {
        std::cerr << "Cannot listen on port " << demo_port << ": " << e.what() << '\n';
        return 1;
    }
}
