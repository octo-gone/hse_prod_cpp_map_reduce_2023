find_package(Python3 COMPONENTS Interpreter REQUIRED)

# define tests
enable_testing()
file(GLOB filenames "${CMAKE_CURRENT_SOURCE_DIR}/test_*.py")
list(SORT filenames)

foreach(filename IN LISTS filenames)
    get_filename_component(unittest ${filename} NAME_WE)
    add_test(${unittest} ${Python3_EXECUTABLE} ${filename})
endforeach()