// RPC server: exposes a few functions over msgpack-RPC on port 8080.
//
// Debugging tips:
//   - Set a breakpoint inside any lambda below to see a request being handled.
//   - Look at the Call Stack window: the call comes from rpclib's dispatcher
//     (rpclib/lib/rpc/dispatcher.cc) on one of the server's worker threads.

#include <rpc/server.h>
#include <rpc/this_handler.h>

#include <chrono>
#include <iostream>
#include <string>
#include <thread>

int main() {
    constexpr uint16_t port = 8080;
    rpc::server srv(port);

    // Simple synchronous function
    srv.bind("add", [](int a, int b) {
        return a + b;
    });

    // Slow function: simulates work taking `delay_ms` milliseconds,
    // so the client can demonstrate async calls finishing in different order.
    srv.bind("slow_square", [](int x, int delay_ms) {
        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
        return x * x;
    });

    // Function that reports an error back to the client.
    // On the client side, the error arrives through the promise as an exception.
    srv.bind("divide", [](double a, double b) {
        if (b == 0.0) {
            rpc::this_handler().respond_error("division by zero");
        }
        return a / b;
    });

    srv.bind("echo", [](const std::string& text) {
        std::cout << "echo: " << text << '\n';
        return text;
    });

    std::cout << "Server listening on port " << port << "\n";

    // Use several worker threads so slow calls can run in parallel.
    // Use srv.run() instead for a single-threaded server (simpler to debug).
    srv.async_run(4);

    std::cout << "Press Enter to stop the server...\n";
    std::cin.get();
    srv.stop();
}
