// Small helpers shared by promise_server and promise_client:
//   - portable socket includes (Winsock on Windows, BSD sockets elsewhere)
//   - message framing: [4-byte id][4-byte length][payload], big-endian
#pragma once

#include <cstdint>
#include <string>

#ifdef _WIN32
  #ifndef WIN32_LEAN_AND_MEAN
    #define WIN32_LEAN_AND_MEAN
  #endif
  #ifndef NOMINMAX
    #define NOMINMAX
  #endif
  #include <winsock2.h>
  #include <ws2tcpip.h>
  using socket_t = SOCKET;
  constexpr socket_t invalid_socket = INVALID_SOCKET;
  inline void close_socket(socket_t s) { ::closesocket(s); }
#else
  #include <arpa/inet.h>
  #include <netinet/in.h>
  #include <sys/select.h>
  #include <sys/socket.h>
  #include <unistd.h>
  using socket_t = int;
  constexpr socket_t invalid_socket = -1;
  inline void close_socket(socket_t s) { ::close(s); }
#endif

constexpr uint16_t demo_port = 9090;

// Winsock must be initialized once per process; this object does it (no-op elsewhere).
struct net_init {
#ifdef _WIN32
    net_init()  { 
        WSADATA d; 
        ::WSAStartup(MAKEWORD(2, 2), &d); 
    }
    ~net_init() { ::WSACleanup(); }
#endif
};

// First argument of select(): ignored on Windows, highest socket + 1 elsewhere.
inline int select_nfds(socket_t highest) {
#ifdef _WIN32
    (void)highest;
    return 0;
#else
    return highest + 1;
#endif
}

// Wait up to `timeout_ms` for `s` to become readable. Returns true if readable.
inline bool wait_readable(socket_t s, int timeout_ms) {
    fd_set set;
    FD_ZERO(&set);
    FD_SET(s, &set);
    timeval tv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
    return ::select(select_nfds(s), &set, nullptr, nullptr, &tv) > 0;
}

// ---- framing ----------------------------------------------------------------

inline void put_u32(std::string& out, uint32_t v) {
    out.push_back(static_cast<char>((v >> 24) & 0xFF));
    out.push_back(static_cast<char>((v >> 16) & 0xFF));
    out.push_back(static_cast<char>((v >> 8) & 0xFF));
    out.push_back(static_cast<char>(v & 0xFF));
}

inline uint32_t get_u32(const std::string& in, std::size_t pos) {
    return (uint32_t(uint8_t(in[pos])) << 24) | (uint32_t(uint8_t(in[pos + 1])) << 16) |
           (uint32_t(uint8_t(in[pos + 2])) << 8) | uint32_t(uint8_t(in[pos + 3]));
}

inline std::string encode_frame(uint32_t id, const std::string& payload) {
    std::string frame;
    put_u32(frame, id);
    put_u32(frame, static_cast<uint32_t>(payload.size()));
    frame += payload;
    return frame;
}

// If `buffer` starts with a complete frame, extract it (removing it from the
// buffer) and return true. Otherwise leave the buffer unchanged, return false.
inline bool try_decode_frame(std::string& buffer, uint32_t& id, std::string& payload) {
    if (buffer.size() < 8) return false;
    uint32_t length = get_u32(buffer, 4);
    if (buffer.size() < 8 + length) return false;
    id = get_u32(buffer, 0);
    payload = buffer.substr(8, length);
    buffer.erase(0, 8 + length);
    return true;
}

// ---- socket I/O ---------------------------------------------------------------

// Send the whole string (blocking). Returns false on error.
inline bool send_all(socket_t s, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        int n = ::send(s, data.data() + sent, static_cast<int>(data.size() - sent), 0);
        if (n <= 0) return false;
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

// Read whatever is available and append it to `buffer`.
// Returns bytes read, 0 if the peer closed the connection, < 0 on error.
inline int recv_some(socket_t s, std::string& buffer) {
    char chunk[4096];
    int n = ::recv(s, chunk, sizeof(chunk), 0);
    if (n > 0) buffer.append(chunk, static_cast<std::size_t>(n));
    return n;
}
