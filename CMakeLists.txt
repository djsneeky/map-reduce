cmake_minimum_required(VERSION 3.14)
project(map-reduce)

# GoogleTest requires at least C++14
set(CMAKE_CXX_STANDARD 14)

enable_testing()

add_subdirectory(src)
add_subdirectory(test)
