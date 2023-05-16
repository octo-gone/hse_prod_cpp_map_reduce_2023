project(map_reduce)

set(MAP_REDUCE_SRC_FILES src/map_reduce.cpp)

add_library(map_reduce_lib STATIC ${MAP_REDUCE_SRC_FILES})
target_link_libraries(map_reduce_lib pthread)