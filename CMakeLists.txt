cmake_minimum_required(VERSION 3.14)
project(map-reduce)

# GoogleTest requires at least C++14
set(CMAKE_CXX_STANDARD 14)

include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG release-1.12.1
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

include_directories(src)

add_subdirectory(src)
add_subdirectory(test)

enable_testing()

add_executable(
  map-reduce
  main.cpp
  MapReduce.cpp
  MapReduce.h
)
target_link_libraries(
  map-reduce
  GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(map-reduce)