cmake_minimum_required(VERSION 3.16)
project(analyzer CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(RDKAFKA REQUIRED rdkafka++)

add_executable(analyzer main.cpp)
target_include_directories(analyzer PRIVATE ${RDKAFKA_INCLUDE_DIRS})
target_link_libraries(analyzer PRIVATE ${RDKAFKA_LIBRARIES})
target_compile_options(analyzer PRIVATE ${RDKAFKA_CFLAGS_OTHER})
