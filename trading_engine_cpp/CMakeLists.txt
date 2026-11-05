cmake_minimum_required(VERSION 3.16)
project(trading_engine_cpp CXX)

# Default to an optimised Release build. Without this, a plain "cmake .."
# on a fresh machine picks no build type at all, which means no
# optimisation - the recorders would quietly run several times slower.
# Pass -DCMAKE_BUILD_TYPE=Debug to get a debug build on purpose.
# (CMAKE_CONFIGURATION_TYPES is set by Xcode/Visual Studio, which choose
# the build type themselves, so it's left alone there.)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS_RELEASE "-O3")

# IXWebSocket defines USE_TLS as an option() internally, defaulting to
# FALSE. Setting it as a CACHE variable here, BEFORE FetchContent pulls
# it in, is how a parent project overrides a dependency's own option()
# — option() only applies its default if the variable isn't already
# set, so this takes priority. On Apple this picks SecureTransport
# automatically (no OpenSSL needed); on Linux it'll fall back to
# OpenSSL (will need libssl-dev installed there later).
set(USE_TLS ON CACHE BOOL "Enable TLS support in IXWebSocket" FORCE)

include(FetchContent)
FetchContent_Declare(
    ixwebsocket
    GIT_REPOSITORY https://github.com/machinezone/IXWebSocket.git
    GIT_TAG v12.1.0
)
FetchContent_MakeAvailable(ixwebsocket)

# zstd (Zstandard) compresses finished hourly files. It's Facebook's
# official C library, pinned to a release tag like IXWebSocket, so the Mac
# and the Linux server build exactly the same code. Its CMake build lives
# in build/cmake rather than the repo root, hence SOURCE_SUBDIR. Only the
# static library is needed - not the zstd command-line tool, the shared
# library or zstd's own tests - so those are switched off to keep the
# build small. Legacy support (reading zstd's pre-2016 formats) is off too:
# we only ever read files we wrote ourselves.
set(ZSTD_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
set(ZSTD_LEGACY_SUPPORT OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(ZSTD_BUILD_STATIC ON CACHE BOOL "" FORCE)
set(ZSTD_BUILD_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    zstd
    GIT_REPOSITORY https://github.com/facebook/zstd.git
    GIT_TAG v1.5.7
    SOURCE_SUBDIR build/cmake
)
FetchContent_MakeAvailable(zstd)

# Threads::Threads is the portable way to ask for thread support: a no-op
# on macOS, but it adds -pthread on Linux, where omitting it makes
# std::thread fail at link time. Found up here, before any target, because
# CMake reads top to bottom and every target that uses the Recorder (which
# starts a std::thread) needs it.
find_package(Threads REQUIRED)

add_executable(main
    src/main.cpp
    third_party/simdjson/simdjson.cpp
)

target_include_directories(main PRIVATE third_party/simdjson)
target_link_libraries(main PRIVATE ixwebsocket)

add_executable(binance_ws
    src/binance_ws.cpp
    third_party/simdjson/simdjson.cpp
    src/recorder.cpp
    src/hourly_file_sink.cpp
    src/runtime.cpp
    src/backoff.cpp
    src/reconnector.cpp
)

target_link_libraries(binance_ws PRIVATE ixwebsocket Threads::Threads)
target_include_directories(binance_ws PRIVATE third_party/simdjson)

add_executable(get_market
    src/get_market_main.cpp
    src/get_market.cpp
    third_party/simdjson/simdjson.cpp
)

target_link_libraries(get_market PRIVATE ixwebsocket)
target_include_directories(get_market PRIVATE third_party/simdjson)

add_executable(clob_ws
    src/clob_ws.cpp
    third_party/simdjson/simdjson.cpp
    src/get_market.cpp
    src/recorder.cpp
    src/hourly_file_sink.cpp
    src/runtime.cpp
    src/backoff.cpp
    src/reconnector.cpp
)

target_link_libraries(clob_ws PRIVATE ixwebsocket Threads::Threads)
target_include_directories(clob_ws PRIVATE third_party/simdjson)

add_executable(polybolt_ws
    src/polybolt_ws.cpp
    src/runtime.cpp
    src/recorder.cpp
    src/hourly_file_sink.cpp
    src/backoff.cpp
    src/reconnector.cpp
    third_party/simdjson/simdjson.cpp
)

target_link_libraries(polybolt_ws PRIVATE ixwebsocket Threads::Threads)
target_include_directories(polybolt_ws PRIVATE third_party/simdjson)

# The Recorder needs no websocket or JSON library - it only touches the
# standard library, so this target builds in a second.

add_executable(recorder_test
    src/recorder_test.cpp
    src/recorder.cpp
)

target_link_libraries(recorder_test PRIVATE Threads::Threads)

# Backoff is pure standard-library maths - no threads, network or JSON.
add_executable(backoff_test
    src/backoff_test.cpp
    src/backoff.cpp
)

# HourlyFileSink only uses the standard library and the operating
# system's file calls, so it builds on its own too.
add_executable(hourly_file_sink_test
    src/hourly_file_sink_test.cpp
    src/hourly_file_sink.cpp
)

# compress_file: the "compress one finished hour safely" step. Linking
# libzstd_static also brings in zstd's headers.
add_executable(compress_file_test
    src/compress_file_test.cpp
    src/compress_file.cpp
)

target_link_libraries(compress_file_test PRIVATE libzstd_static)

# compress_hours: one pass over the data folder, compressing every
# finished hour. Separate from the recorders so heavy compression can run
# at low priority and can never crash live recording.
add_executable(compress_hours
    src/compress_hours.cpp
    src/compress_pass.cpp
    src/compress_file.cpp
)

target_link_libraries(compress_hours PRIVATE libzstd_static)

# Also builds hourly_file_sink.cpp, to check compress_hours understands
# exactly the file names the recorders write.
add_executable(compress_hours_test
    src/compress_hours_test.cpp
    src/compress_pass.cpp
    src/compress_file.cpp
    src/hourly_file_sink.cpp
)

target_link_libraries(compress_hours_test PRIVATE libzstd_static)


