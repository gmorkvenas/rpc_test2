cmake_minimum_required(VERSION 3.15)
project(rpc_test CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# rpclib is built from source as part of this project, so the debugger
# has full symbols and sources for it.
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/rpclib/CMakeLists.txt")
    add_subdirectory(rpclib)
else()
    # Fallback: download rpclib automatically if the folder is missing
    include(FetchContent)
    FetchContent_Declare(rpclib
        GIT_REPOSITORY https://github.com/rpclib/rpclib.git
        GIT_TAG        master)
    FetchContent_MakeAvailable(rpclib)
endif()

add_executable(server src/server.cpp)
target_link_libraries(server PRIVATE rpc)

add_executable(client src/client.cpp)
target_link_libraries(client PRIVATE rpc)

# ---------------------------------------------------------------
# promise_demo: the std::promise pattern from "C++ Concurrency in Action"
# implemented with plain sockets (no rpclib).
# ---------------------------------------------------------------
find_package(Threads REQUIRED)

add_executable(promise_server src/promise_demo/promise_server.cpp)
add_executable(promise_client src/promise_demo/promise_client.cpp)
target_link_libraries(promise_client PRIVATE Threads::Threads)

if(WIN32)
    target_link_libraries(promise_server PRIVATE ws2_32)
    target_link_libraries(promise_client PRIVATE ws2_32)
endif()
