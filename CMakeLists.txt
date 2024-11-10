cmake_minimum_required(VERSION 3.16)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON) # Require C++17 support.

# set(CLANG_SEARCH_PATH "/usr/local/bin" "/usr/bin" "/usr/local/opt/llvm/bin" "/usr/local/opt/llvm@14/bin")

set(CMAKE_C_COMPILER "/usr/bin/clang")
set(CMAKE_CXX_COMPILER "/usr/bin/clang++")

project(EasyDB
        LANGUAGES C CXX
)

# #####################################################################################################################
# DEPENDENCIES
# #####################################################################################################################

# CTest
enable_testing()

# #####################################################################################################################
# COMPILER SETUP
# #####################################################################################################################

set(ASAN_OPTIONS detect_leaks=0)
# Compiler flags.
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -Wextra")
# set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Werror")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -ggdb -fsanitize=address -fno-omit-frame-pointer -fno-optimize-sibling-calls")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Output directory.
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Includes.
set(EasyDB_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
set(EasyDB_TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/test/include)
set(EasyDB_DEPS_INCLUDE_DIR
        ${PROJECT_SOURCE_DIR}/deps
        # ${PROJECT_SOURCE_DIR}/deps/fmt/include
        # ${PROJECT_SOURCE_DIR}/deps/libpg_query/include
        # ${PROJECT_SOURCE_DIR}/deps/argparse/include
        # ${PROJECT_SOURCE_DIR}/deps/cpp_random_distributions
        # ${PROJECT_SOURCE_DIR}/deps/backward-cpp
)

include_directories(${EasyDB_SRC_INCLUDE_DIR} ${EasyDB_TEST_INCLUDE_DIR} ${EasyDB_DEPS_INCLUDE_DIR})
# include_directories(${EasyDB_SRC_INCLUDE_DIR} ${EasyDB_DEPS_INCLUDE_DIR})
include_directories(BEFORE src) # This is needed for gtest.

# #####################################################################################################################
# Other CMake modules
# MUST BE ADDED AFTER CONFIGURING COMPILER PARAMETERS
# #####################################################################################################################

add_subdirectory(deps)
add_subdirectory(src)
add_subdirectory(test)
# add_subdirectory(tools)
# add_subdirectory(client)