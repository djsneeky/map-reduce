include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG release-1.12.1
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

set(TEST_BINARY ${CMAKE_PROJECT_NAME}_test)
file(GLOB_RECURSE TEST_SOURCES LIST_DIRECTORIES false *.h *.cpp)
add_executable(${TEST_BINARY} ${TEST_SOURCES})
# add_test(NAME ${BINARY} COMMAND ${BINARY})
target_link_libraries(${TEST_BINARY} ${CMAKE_PROJECT_NAME}_lib GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(${TEST_BINARY})