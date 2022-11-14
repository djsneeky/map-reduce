set(BINARY ${CMAKE_PROJECT_NAME})
file(GLOB_RECURSE SOURCES LIST_DIRECTORIES true *.h *.cpp)
set(SOURCES ${SOURCES})
add_executable(${BINARY}_run ${SOURCES})
add_library(${BINARY}_lib STATIC ${SOURCES})

# TODO: Add target compiler features for OpenMP and MPI
# target_compile_features(modern_library PUBLIC cxx_std_11)