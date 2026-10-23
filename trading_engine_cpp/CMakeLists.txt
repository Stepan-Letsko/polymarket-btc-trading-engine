cmake_minimum_required(VERSION 3.16)
project(trading_engine_cpp CXX)

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

add_executable(main
    src/main.cpp
    third_party/simdjson/simdjson.cpp
)

target_include_directories(main PRIVATE third_party/simdjson)
target_link_libraries(main PRIVATE ixwebsocket)

add_executable(binance_ws
    src/binance_ws.cpp
    third_party/simdjson/simdjson.cpp
)

target_link_libraries(binance_ws PRIVATE ixwebsocket)
target_include_directories(binance_ws PRIVATE third_party/simdjson)

add_executable(get_market
    src/get_market.cpp
    third_party/simdjson/simdjson.cpp
)

target_link_libraries(get_market PRIVATE ixwebsocket)
target_include_directories(get_market PRIVATE third_party/simdjson)
