project(app)

set(APP_SRC_FILES src/main.cpp)

add_executable(app ${APP_SRC_FILES})
target_link_libraries(app PRIVATE map_reduce_lib)

set_target_properties(app PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)

