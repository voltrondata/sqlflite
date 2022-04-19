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