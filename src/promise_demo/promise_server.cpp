// promise_server: single-threaded server for the promise demo.
//
// One thread loops over all connections (like process_connections in the
// client), using select() to see which sockets have data.
//
// Protocol: every request frame carries an id; the reply uses the same id.
// Commands (payload text):
//   upper:<text>          -> text in upper case
//   add:<a>,<b>           -> a + b
//   delay:<ms> <text>     -> text, sent back after <ms> milliseconds
// Because of "delay", replies can go out in a DIFFERENT order than the
// requests arrived. The client matches them to the right promise by id.

#include "net.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

using clock_type = std::chrono::steady_clock;

struct client_connection {
    socket_t    sock;
    std::string read_buffer;   // bytes received but not yet parsed into frames
};

struct scheduled_reply {
    clock_type::time_point due;   // when to send it
    socket_t               sock;
    uint32_t               id;
    std::string            payload;
};

// Execute one command. Sets `delay_ms` for delayed replies.
std::string handle_command(const std::string& request, int& delay_ms) {
    delay_ms = 0;
    auto starts_with = [&](const char* prefix) { return request.rfind(prefix, 0) == 0; };

    if (starts_with("upper:")) {
        std::string text = request.substr(6);
        std::transform(text.begin(), text.end(), text.begin(),
                       [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
        return text;
    }
    if (starts_with("add:")) {
        int a = 0, b = 0;
        char comma = 0;
        std::string args = request.substr(4);
        if (std::sscanf(args.c_str(), "%d%c%d", &a, &comma, &b) == 3 && comma == ',')
            return std::to_string(a + b);
        return "error: expected add:<a>,<b>";
    }
    if (starts_with("delay:")) {
        std::string rest = request.substr(6);
        std::size_t space = rest.find(' ');
        delay_ms = std::stoi(rest.substr(0, space));
        return space == std::string::npos ? "" : rest.substr(space + 1);
    }
    return "error: unknown command '" + request + "'";
}

int main() {
    [[maybe_unused]] net_init net;   // starts Winsock on Windows

    socket_t listener = ::socket(AF_INET, SOCK_STREAM, 0);
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
    std::vector<scheduled_reply>   replies;

    while (true) {
        // 1. Wait (max 10 ms) until the listener or any connection has data
        fd_set readable;
        FD_ZERO(&readable);
        FD_SET(listener, &readable);
        socket_t highest = listener;
        for (auto& c : connections) {
            FD_SET(c.sock, &readable);
            highest = std::max(highest, c.sock);
        }
        timeval tv{0, 10 * 1000};
        ::select(select_nfds(highest), &readable, nullptr, nullptr, &tv);

        // 2. New connection?
        if (FD_ISSET(listener, &readable)) {
            socket_t s = ::accept(listener, nullptr, nullptr);
            if (s != invalid_socket) {
                connections.push_back({s, {}});
                std::cout << "client connected\n";
            }
        }

        // 3. Incoming data on each connection
        for (auto it = connections.begin(); it != connections.end();) {
            if (FD_ISSET(it->sock, &readable) && recv_some(it->sock, it->read_buffer) <= 0) {
                std::cout << "client disconnected\n";
                socket_t gone = it->sock;
                replies.erase(std::remove_if(replies.begin(), replies.end(),
                                             [gone](const scheduled_reply& r) { return r.sock == gone; }),
                              replies.end());
                close_socket(gone);
                it = connections.erase(it);
                continue;
            }
            uint32_t id;
            std::string request;
            while (try_decode_frame(it->read_buffer, id, request)) {
                int delay_ms = 0;
                std::string result;
                try {
                    result = handle_command(request, delay_ms);
                } catch (const std::exception&) {       // e.g. bad number in "delay:"
                    result = "error: bad request '" + request + "'";
                }
                std::cout << "  request #" << id << " '" << request << "'"
                          << (delay_ms ? " (reply in " + std::to_string(delay_ms) + " ms)" : "") << '\n';
                replies.push_back({clock_type::now() + std::chrono::milliseconds(delay_ms),
                                   it->sock, id, result});
            }
            ++it;
        }

        // 4. Outgoing: send every reply whose time has come
        auto now = clock_type::now();
        for (auto it = replies.begin(); it != replies.end();) {
            if (it->due <= now) {
                send_all(it->sock, encode_frame(it->id, it->payload));
                std::cout << "  reply   #" << it->id << " '" << it->payload << "'\n";
                it = replies.erase(it);
            } else {
                ++it;
            }
        }
    }
}
