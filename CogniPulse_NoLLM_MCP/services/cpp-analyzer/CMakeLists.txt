cmake_minimum_required(VERSION 3.16)
project(cognipulse_analyzer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(cognipulse_analyzer main.cpp)

find_package(PkgConfig REQUIRED)

pkg_check_modules(RDKAFKA REQUIRED rdkafka)
pkg_check_modules(RDKAFKACPP REQUIRED rdkafka++)

target_include_directories(cognipulse_analyzer PRIVATE
    ${RDKAFKA_INCLUDE_DIRS}
    ${RDKAFKACPP_INCLUDE_DIRS}
)

target_link_libraries(cognipulse_analyzer PRIVATE
    ${RDKAFKA_LIBRARIES}
    ${RDKAFKACPP_LIBRARIES}
)

find_package(PostgreSQL REQUIRED)
target_include_directories(cognipulse_analyzer PRIVATE ${PostgreSQL_INCLUDE_DIRS})
target_link_libraries(cognipulse_analyzer PRIVATE ${PostgreSQL_LIBRARIES})
