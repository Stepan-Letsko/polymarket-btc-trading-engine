cmake_minimum_required(VERSION 3.16)
project(engine CXX)

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

# ---------------------------------------------------------------------------
# Libraries: one per folder in src/, each compiled once and shared by every
# program that needs it. Includes are written from src/
# ("recording/recorder.h"), so src/ is every library's include folder -
# PUBLIC, so whatever links a library can include its headers too.
# ---------------------------------------------------------------------------

# simdjson, the JSON parser: one large .cpp, compiled once here rather
# than once per program.
add_library(simdjson STATIC third_party/simdjson/simdjson.cpp)
target_include_directories(simdjson PUBLIC third_party/simdjson)

# common: retry timing (backoff), shutdown signals and clocks (runtime),
# and websocket reconnecting (reconnector, built on IXWebSocket).
add_library(common STATIC
    src/common/backoff.cpp
    src/common/runtime.cpp
    src/common/reconnector.cpp
)
target_include_directories(common PUBLIC src)
target_link_libraries(common PUBLIC ixwebsocket)

# recording: how records get to disk - the Recorder's queue and writer
# thread, and the hourly files it writes into.
add_library(recording STATIC
    src/recording/recorder.cpp
    src/recording/hourly_file_sink.cpp
)
target_include_directories(recording PUBLIC src)
target_link_libraries(recording PUBLIC Threads::Threads)

# compression: turning a finished hour into a verified .zst.
add_library(compression STATIC
    src/compression/compress_file.cpp
    src/compression/compress_pass.cpp
)
target_include_directories(compression PUBLIC src)
target_link_libraries(compression PRIVATE libzstd_static)

# feeds: exchange-specific helpers - for now, looking up the current
# Polymarket BTC 5m market through the Gamma API.
add_library(feeds STATIC src/feeds/get_market.cpp)
target_include_directories(feeds PUBLIC src)
target_link_libraries(feeds PRIVATE ixwebsocket simdjson)

# ---------------------------------------------------------------------------
# Programs: one file each in apps/, wiring the libraries together.
# ---------------------------------------------------------------------------

add_executable(binance_ws apps/binance_ws.cpp)
target_link_libraries(binance_ws PRIVATE common recording simdjson ixwebsocket)

add_executable(clob_ws apps/clob_ws.cpp)
target_link_libraries(clob_ws PRIVATE common recording feeds simdjson ixwebsocket)

add_executable(polybolt_ws apps/polybolt_ws.cpp)
target_link_libraries(polybolt_ws PRIVATE common recording simdjson ixwebsocket)

add_executable(get_market apps/get_market.cpp)
target_link_libraries(get_market PRIVATE feeds)

# compress_hours: one pass over the data folder, compressing every
# finished hour. Separate from the recorders so heavy compression can run
# at low priority and can never crash live recording.
# Checks --level against zstd's own maximum, so it links zstd directly too.
add_executable(compress_hours apps/compress_hours.cpp)
target_link_libraries(compress_hours PRIVATE compression libzstd_static)

# ---------------------------------------------------------------------------
# Tests: one file each in tests/. Each prints "all checks passed" and
# exits 0, or names the failing check and exits 1.
# ---------------------------------------------------------------------------

add_executable(recorder_test tests/recorder_test.cpp)
target_link_libraries(recorder_test PRIVATE recording)

add_executable(backoff_test tests/backoff_test.cpp)
target_link_libraries(backoff_test PRIVATE common)

add_executable(hourly_file_sink_test tests/hourly_file_sink_test.cpp)
target_link_libraries(hourly_file_sink_test PRIVATE recording)

# Reads zstd frame headers itself, so it links zstd directly as well.
add_executable(compress_file_test tests/compress_file_test.cpp)
target_link_libraries(compress_file_test PRIVATE compression libzstd_static)

# Also links recording, to check compress_hours understands exactly the
# file names the recorders write.
add_executable(compress_hours_test tests/compress_hours_test.cpp)
target_link_libraries(compress_hours_test PRIVATE compression recording)
