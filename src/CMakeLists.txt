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

find_package(MPI REQUIRED)
message(STATUS "Run: ${MPIEXEC} ${MPIEXEC_NUMPROC_FLAG} ${MPIEXEC_MAX_NUMPROCS} ${MPIEXEC_PREFLAGS} EXECUTABLE ${MPIEXEC_POSTFLAGS} ARGS")
target_link_libraries(${SRC_LIB} PUBLIC MPI::MPI_CXX)
target_link_libraries(${BINARY}  PUBLIC MPI::MPI_CXX)


# Add plot lib
include(FetchContent)
FetchContent_Declare(matplotplusplus
        GIT_REPOSITORY https://github.com/alandefreitas/matplotplusplus
        GIT_TAG origin/master) # or whatever tag you want
FetchContent_GetProperties(matplotplusplus)
if(NOT matplotplusplus_POPULATED)
    FetchContent_Populate(matplotplusplus)
    add_subdirectory(${matplotplusplus_SOURCE_DIR} ${matplotplusplus_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

target_link_libraries(${SRC_LIB} PUBLIC matplot)
target_link_libraries(${BINARY} PUBLIC matplot)
        

# TODO: Add target compiler features for OpenMP and MPI
# target_compile_features(modern_library PUBLIC cxx_std_11)