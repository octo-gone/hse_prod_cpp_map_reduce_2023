cmake_minimum_required(VERSION 3.10)

project(map_reduce VERSION 1.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
add_compile_options(-Wall -Wpedantic -Werror -O3)

enable_testing()
include_directories(${CMAKE_SOURCE_DIR}/include)
add_subdirectory(libsrc/map_reduce)
add_subdirectory(projects/app)
add_subdirectory(test/libsrc/map_reduce)