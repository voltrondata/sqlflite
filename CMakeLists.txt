cmake_minimum_required(VERSION 3.10)

# set the project name
project(flight_sql)

set(ArrowFlight_DIR /usr/lib/x86_64-linux-gnu/cmake/arrow)

find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED)

# find duckDB
find_library(DUCKDB_LIB NAMES duckdb PATHS /usr/lib/)
set(DUCKDB ${DUCKDB_LIB})

# set sources
set(SOURCE ${CMAKE_CURRENT_SOURCE_DIR}/src/flight_sql.cpp)
  
# add_subdirectory("${PROJECT_SOURCE_DIR}/folder1")

# add the executable
add_executable(flight_sql ${SOURCE})
target_link_libraries(flight_sql ${DUCKDB})

