cmake_minimum_required(VERSION 3.10)
project(analyzer)

set(CMAKE_CXX_STANDARD 17)

find_package(PkgConfig REQUIRED)
pkg_check_modules(RDKAFKA REQUIRED rdkafka++)
find_package(nlohmann_json 3.2.0 REQUIRED)

include_directories(${RDKAFKA_INCLUDE_DIRS})

add_executable(analyzer src/analyzer.cpp)

target_link_libraries(analyzer
    ${RDKAFKA_LIBRARIES}
    nlohmann_json::nlohmann_json
)
