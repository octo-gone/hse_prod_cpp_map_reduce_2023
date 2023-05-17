set(MAP_REDUCE_TEST_SRC_FILES src/test.cpp)

# Fetch GTEST
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.13.0.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Configure tests executable
include(GoogleTest)
add_executable(map_reduce_test ${MAP_REDUCE_TEST_SRC_FILES})
target_link_libraries(map_reduce_test GTest::gtest_main)

set_target_properties(map_reduce_test PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)

# Discover tests
gtest_discover_tests(map_reduce_test)