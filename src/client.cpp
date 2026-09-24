// RPC client: demonstrates synchronous calls, asynchronous calls with
// std::future, waiting for the first result, and error handling.
//
// Debugging tips (how rpclib uses std::promise internally):
//   - async_call() creates a std::promise, stores it in a map keyed by the
//     call ID (rpclib/lib/rpc/client.cc, member `ongoing_calls_`), sends the
//     request and returns the promise's future.
//   - rpclib's I/O thread reads responses, finds the promise with the matching
//     ID and calls set_value() (or set_exception() on error).
//   - Set breakpoints in client.cc around ongoing_calls_ to watch this happen,
//     and use Debug > Windows > Threads to see the I/O thread.

#include <rpc/client.h>
#include <rpc/rpc_error.h>

#include <chrono>
#include <future>
#include <iostream>
#include <string>
#include <vector>

// Wait until one of the futures is ready and return its index.
// (Standard C++ has no when_any, so we poll with a short timeout.)
std::size_t wait_any(std::vector<std::future<clmdep_msgpack::object_handle>>& futures) {
    while (true) {
        for (std::size_t i = 0; i < futures.size(); ++i) {
            if (futures[i].valid() &&
                futures[i].wait_for(std::chrono::milliseconds(1)) == std::future_status::ready) {
                return i;
            }
        }
    }
}

int main() {
    try {
        rpc::client c("127.0.0.1", 8080);

        // 1. Synchronous call: blocks until the result arrives
        int sum = c.call("add", 2, 3).as<int>();
        std::cout << "add(2, 3) = " << sum << '\n';

        // 2. Asynchronous call: returns a std::future immediately
        auto fut = c.async_call("add", 10, 20);
        std::cout << "async add(10, 20) sent, waiting...\n";
        std::cout << "async add(10, 20) = " << fut.get().as<int>() << '\n';

        // 3. Several async calls at once; the server finishes them in a
        //    different order than they were sent (different delays).
        std::vector<int> delays = {900, 300, 600};
        std::vector<std::future<clmdep_msgpack::object_handle>> futures;
        for (std::size_t i = 0; i < delays.size(); ++i) {
            futures.push_back(c.async_call("slow_square", static_cast<int>(i + 2), delays[i]));
        }

        auto start = std::chrono::steady_clock::now();
        std::size_t first = wait_any(futures);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - start).count();
        std::cout << "First result: call #" << first
                  << " (delay " << delays[first] << " ms) = "
                  << futures[first].get().as<int>()
                  << ", after " << ms << " ms\n";

        // Collect the rest
        for (std::size_t i = 0; i < futures.size(); ++i) {
            if (i != first) {
                std::cout << "Call #" << i << " = " << futures[i].get().as<int>() << '\n';
            }
        }

        // 4. Error from the server arrives as an exception
        try {
            double r = c.call("divide", 1.0, 0.0).as<double>();
            std::cout << "divide = " << r << '\n';
        } catch (rpc::rpc_error& e) {  // non-const: rpclib's get_error() is not const
            std::cout << "Server error in '" << e.get_function_name() << "': "
                      << e.get_error().as<std::string>() << '\n';
        }

        c.call("echo", std::string("Hello from client"));
        std::cout << "Done.\n";
    } catch (const std::exception& e) {
        std::cerr << "Client error: " << e.what() << '\n';
        return 1;
    }
}
