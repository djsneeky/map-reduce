# Create executable for primary application
set(BINARY ${CMAKE_PROJECT_NAME}_app)
file(GLOB_RECURSE SOURCES LIST_DIRECTORIES true *.h *.cpp)
add_executable(${BINARY} ${SOURCES})

# Create library of sources minus main.cpp for testing
set(SRC_LIB ${CMAKE_PROJECT_NAME}_lib)
add_library(${SRC_LIB} STATIC MapReduce.cpp)
target_include_directories(${SRC_LIB} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

if(NOT TARGET OpenMP::OpenMP_CXX)
    find_package(Threads REQUIRED)
    add_library(OpenMP::OpenMP_CXX IMPORTED INTERFACE)
    set_property(TARGET OpenMP::OpenMP_CXX
                 PROPERTY INTERFACE_COMPILE_OPTIONS ${OpenMP_CXX_FLAGS})
    # Only works if the same flag is passed to the linker; use CMake 3.9+ otherwise (Intel, AppleClang)
    set_property(TARGET OpenMP::OpenMP_CXX
                 PROPERTY INTERFACE_LINK_LIBRARIES ${OpenMP_CXX_FLAGS} Threads::Threads)

endif()
target_link_libraries(${SRC_LIB} PUBLIC OpenMP::OpenMP_CXX)
target_link_libraries(${BINARY} PUBLIC OpenMP::OpenMP_CXX)

# TODO: Add target compiler features for OpenMP and MPI
# target_compile_features(modern_library PUBLIC cxx_std_11)