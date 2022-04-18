# cmake_minimum_required(VERSION 3.10)
# # set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")

# # set the project name
# project(flight_sql)

# # set(Libraries_DIR /usr/lib/x86_64-linux-gnu)

# # # set(ArrowFlight_DIR ${Libraries_DIR}/cmake/arrow)
# # # set(ArrowFlightSql_DIR ${Libraries_DIR}/cmake/arrow)
# # set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} 
# #     "/home/tom/miniconda3/envs/arrow-dev/lib/x86_64-linux-gnu"  
# #     "/home/tom/miniconda3/envs/arrow-dev/lib/x86_64-linux-gnu/cmake/arrow"
# # )

# # set(CMAKE_INCLUDE_PATH ${CMAKE_INCLUDE_PATH} "/home/tom/miniconda3/envs/arrow-dev/lib/x86_64-linux-gnu/include")

# # set(THREADS_PREFER_PTHREAD_FLAG ON)
# # find_package(Threads REQUIRED)
# find_package(Arrow REQUIRED)
# find_package(ArrowFlight REQUIRED)
# find_package(ArrowFlightSql REQUIRED)
# # find_package(gflags REQUIRED)
# # find_package(ArrowFlightSql REQUIRED)
# # find_package(SQLite3Alt REQUIRED)

# # # set(ARROW_PROTOBUF_LIBPROTOBUF arrow::protobuf::libprotobuf)
# # # set(ARROW_FLIGHT_LINK_LIBS  ${ARROW_PROTOBUF_LIBPROTOBUF})
# # # find duckDB
# # find_library(DUCKDB_LIB NAMES duckdb PATHS /usr/lib/)
# # set(DUCKDB ${DUCKDB_LIB})

# # # add protobuf
# # set(PROTOBUF ${Libraries_DIR}/libprotobuf.so)

# # set sources
# set(SOURCE ${CMAKE_CURRENT_SOURCE_DIR}/src/flight_sql.cpp)
  
# # add_subdirectory("${PROJECT_SOURCE_DIR}/folder1")

# # add the executable
# add_executable(flight_sql ${SOURCE})
# target_link_libraries(flight_sql PRIVATE ${DUCKDB} arrow_shared arrow_flight_shared arrow_flight_sql_shared)
# # target_link_libraries(flight_sql PRIVATE ${DUCKDB} arrow_shared arrow_flight_shared arrow_flight_sql_shared)
# # target_link_libraries(flight_sql PRIVATE arrow_shared)
# # target_link_libraries(arrow-example PRIVATE arrow_static Threads::Threads)

cmake_minimum_required(VERSION 3.10)
project(flight_sql)

find_package(Threads REQUIRED)
find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED)
find_package(ArrowFlightSql REQUIRED)
find_package(SQLite3 REQUIRED)

set(ARROW_FLIGHT_SQL_SQLITE_SERVER_SRCS
    src/sqlite/sqlite_sql_info.cc
    src/sqlite/sqlite_type_info.cc
    src/sqlite/sqlite_statement.cc
    src/sqlite/sqlite_statement_batch_reader.cc
    src/sqlite/sqlite_server.cc
    src/sqlite/sqlite_tables_schema_batch_reader.cc)

add_executable(flight_sql 
    src/flight_sql.cpp 
    ${ARROW_FLIGHT_SQL_SQLITE_SERVER_SRCS}
)

target_include_directories(flight_sql PRIVATE src/sqlite)
target_link_libraries(flight_sql PRIVATE 
		      arrow_shared
		      arrow_flight_shared
		      arrow_flight_sql_shared
              Threads::Threads
              ${SQLite3_LIBRARIES})