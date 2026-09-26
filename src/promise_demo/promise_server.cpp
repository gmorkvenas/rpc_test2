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
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

struct client_connection {
    socket_t    sock;
    std::string read_buffer;   // bytes received but not yet parsed into frames
    std::string description;   // "127.0.0.1:9090 <-> 127.0.0.1:54012  (client 1)"
};

// "127.0.0.1:9090" from an IPv4 address (port is stored big-endian, hence ntohs).
std::string to_string(const sockaddr_in& a) {
    char ip[INET_ADDRSTRLEN];
    ::inet_ntop(AF_INET, &a.sin_addr, ip, sizeof(ip));
    return std::string(ip) + ':' + std::to_string(ntohs(a.sin_port));
}

// Local address of a socket, i.e. our side of the connection.
std::string local_address(socket_t s) {
    sockaddr_in a{};
    socklen_t   len = sizeof(a);
    ::getsockname(s, reinterpret_cast<sockaddr*>(&a), &len);
    return to_string(a);
}

// Print the sockets that are currently in `set`, one per line, e.g.
//   before select:
//     listener(212)  127.0.0.1:9090  (listening)
//     client(344)    127.0.0.1:9090 <-> 127.0.0.1:54012  (client 1)
void print_set(const char* label, const fd_set& set, socket_t listener,
               const std::vector<client_connection>& connections) {
    std::cout << label << ":\n";
    auto name = [](const char* kind, socket_t s) {
        return std::string(kind) + '(' + std::to_string(s) + ')';
    };
    if (FD_ISSET(listener, &set))
        std::cout << "  " << std::left << std::setw(15) << name("listener", listener)
                  << local_address(listener) << "  (listening)\n";
    for (auto& c : connections)
        if (FD_ISSET(c.sock, &set))
            std::cout << "  " << std::left << std::setw(15) << name("client", c.sock)
                      << c.description << '\n';
}

int main() {
    [[maybe_unused]] net_init net;   // starts Winsock on Windows
    std::cout << std::unitbuf;       // flush every output, so logs show up even when redirected to a file

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
    int client_count = 0;   // gives each client a number that stays the same until it disconnects

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

        //pass readable to which ones you expecting;
        // return readable with socket that has generated event
        print_set("before select", readable, listener, connections);
        ::select(select_nfds(highest), &readable, nullptr, nullptr, nullptr);
        print_set("after select ", readable, listener, connections);

        // 2. New connection?
        if (FD_ISSET(listener, &readable)) {
            // accept() also fills in the client's address (its IP and random port)
            sockaddr_in peer{};
            socklen_t   peer_len = sizeof(peer);
            socket_t s = ::accept(listener, reinterpret_cast<sockaddr*>(&peer), &peer_len);
            if (s != invalid_socket) {
                std::string description = local_address(s) + " <-> " + to_string(peer) +
                                          "  (client " + std::to_string(++client_count) + ')';
                connections.push_back({s, {}, description});
                std::cout << "client connected: " << description << '\n';
            }
        }
        else
        {
            std::cout << "listener is not in readable " <<'\n';
        }

        // 3. Incoming data on each connection: answer with echo + "END"
        for (auto it = connections.begin(); it != connections.end();) {
            if (FD_ISSET(it->sock, &readable) && recv_some(it->sock, it->read_buffer) <= 0) {
                std::cout << "client disconnected: " << it->description << '\n';
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
