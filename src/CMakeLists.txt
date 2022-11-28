# Create executable for primary application
set(BINARY ${CMAKE_PROJECT_NAME}_app)
file(GLOB_RECURSE SOURCES LIST_DIRECTORIES true *.h *.cpp)
add_executable(${BINARY} ${SOURCES})

# Create library of sources minus main.cpp for testing
set(SRC_LIB ${CMAKE_PROJECT_NAME}_lib)
add_library(${SRC_LIB} STATIC MapReduce.cpp FileHelper.cpp)
target_include_directories(${SRC_LIB} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

find_package(OpenMP)
if(OpenMP_CXX_FOUND)
    target_link_libraries(${SRC_LIB} PUBLIC OpenMP::OpenMP_CXX)
    target_link_libraries(${BINARY} PUBLIC OpenMP::OpenMP_CXX)
endif()

# TODO: Add target compiler features for OpenMP and MPI
# target_compile_features(modern_library PUBLIC cxx_std_11)