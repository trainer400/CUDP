cmake_minimum_required(VERSION 3.16)
set(CMAKE_MINIMUM_REQUIRED_VERSION ON)
set(CMAKE_CXX_STANDARD 20)

project("CUDP")

set(NETWORK
    ${CMAKE_CURRENT_SOURCE_DIR}/src/network/CUDP.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/network/UDPSocket.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/src/network/MockUDPSocket.cpp
)

include_directories(catch_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

# Tests
find_package(Catch2 3 REQUIRED)
add_executable(catch_tests
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/CircularBufferTests.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/tests/MockUDPSocketTest.cpp
    ${NETWORK}
)
target_link_libraries(catch_tests PRIVATE Catch2::Catch2WithMain)

include(CTest)
include(Catch)
catch_discover_tests(catch_tests)
