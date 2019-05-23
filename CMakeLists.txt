cmake_minimum_required(VERSION 3.2)
project(breeze_cpp)

#set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_STANDARD 17)
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/build/bin)
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/build/lib)
include_directories(${PROJECT_SOURCE_DIR}/src)

set(RPC_SOURCE
        src/breeze.cpp
        src/common/buffer.cpp
        src/test_msg.breeze.cpp)
add_library(breeze ${RPC_SOURCE})

set(UNIT_TEST_SOURCE
        test/test_main.cpp
        test/test_breeze.cpp)
add_executable(unit_test ${UNIT_TEST_SOURCE})
target_link_libraries(unit_test gtest breeze)