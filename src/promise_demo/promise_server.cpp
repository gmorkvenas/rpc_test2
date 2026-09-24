// promise_server: single-threaded echo server for the promise demo.
//
// One thread loops over all connections, using select() to see which
// sockets have data.
//
// Protocol: every request frame carries an id. For each request the server
// sends two reply frames with the same id:
//   1. the request text (echo)
//   2. "END"                    -> tells the client the reply is complete

#include "net.h"

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

struct client_connection {
    socket_t    sock;
    std::string read_buffer;   // bytes received but not yet parsed into frames
};

int main() {
    [[maybe_unused]] net_init net;   // starts Winsock on Windows

    socket_t listener = ::socket(AF_INET, SOCK_STREAM, 0);
    // SO_REUSEADDR lets the server bind port 9090 again right after a restart.
    // Without it, bind() can fail on Linux while the old socket is still in
    // TIME_WAIT (~1-2 minutes). On Windows it also lets a second process bind
    // the same port, so starting two servers by mistake does not fail.
    // The option value is passed as a pointer to an int (1 = on); Winsock
    // declares the parameter as const char*, hence the cast.
    int yes = 1;
    ::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&yes), sizeof(yes));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(demo_port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);   // 127.0.0.1 only

    if (::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
        ::listen(listener, SOMAXCONN) != 0) {
        std::cerr << "Cannot listen on port " << demo_port << " (already in use?)\n";
        return 1;
    }
    std::cout << "promise_server listening on 127.0.0.1:" << demo_port << '\n';

    std::vector<client_connection> connections;

    while (true) {
        // 1. Wait until the listener or any connection has data
        fd_set readable;
        FD_ZERO(&readable);
        //. It adds the listener itself (its socket number) to readable.
        FD_SET(listener, &readable);
        socket_t highest = listener;
        for (auto& c : connections) {
            FD_SET(c.sock, &readable);
            highest = std::max(highest, c.sock);
        }
        // select() blocks until at least one socket in `readable` has data
        // (or a new connection is waiting on the listener). On return it
        // removes the sockets that are NOT ready from the set, so FD_ISSET
        // below tells which ones to read.
        //   arg 1: highest socket + 1 (ignored on Windows)
        //   arg 2: sockets to watch for reading
        //   arg 3: sockets to watch for writing   - not used
        //   arg 4: sockets to watch for errors    - not used
        //   arg 5: timeout; nullptr = wait forever (no other work to do)
        ::select(select_nfds(highest), &readable, nullptr, nullptr, nullptr);

        // 2. New connection?
        if (FD_ISSET(listener, &readable)) {
            socket_t s = ::accept(listener, nullptr, nullptr);
            if (s != invalid_socket) {
                connections.push_back({s, {}});
                std::cout << "client connected\n";
            }
        }

        // 3. Incoming data on each connection: answer with echo + "END"
        for (auto it = connections.begin(); it != connections.end();) {
            if (FD_ISSET(it->sock, &readable) && recv_some(it->sock, it->read_buffer) <= 0) {
                std::cout << "client disconnected\n";
                close_socket(it->sock);
                it = connections.erase(it);
                continue;
            }
            uint32_t id;
            std::string request;
            while (try_decode_frame(it->read_buffer, id, request)) {
                std::cout << "  request #" << id << " '" << request << "'\n";
                send_all(it->sock, encode_frame(id, request));
                send_all(it->sock, encode_frame(id, "END"));
            }
            ++it;
        }
    }
}
