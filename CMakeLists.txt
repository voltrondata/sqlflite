cmake_minimum_required(VERSION 3.10)
project(flight_sql)

set(CMAKE_CXX_STANDARD 17)

list(APPEND CMAKE_PREFIX_PATH dist/lib/cmake)

find_package(Threads REQUIRED)
find_package(Arrow REQUIRED PATHS)
find_package(ArrowFlight REQUIRED)
find_package(ArrowFlightSql REQUIRED)
find_package(SQLite3 REQUIRED)
find_package(Boost COMPONENTS program_options REQUIRED)

# find duckDB
find_library(DUCKDB_LIB NAMES duckdb PATHS /usr/local/lib/)
set(DUCKDB ${DUCKDB_LIB})

set(ARROW_FLIGHT_SQL_SQLITE_SERVER_SRCS
    src/sqlite/sqlite_sql_info.cc
    src/sqlite/sqlite_type_info.cc
    src/sqlite/sqlite_statement.cc
    src/sqlite/sqlite_statement_batch_reader.cc
    src/sqlite/sqlite_server.cc
    src/sqlite/sqlite_tables_schema_batch_reader.cc)

set(ARROW_FLIGHT_SQL_DUCKDB_SERVER_SRCS
    src/duckdb/duckdb_sql_info.cpp
    src/duckdb/duckdb_statement.cpp
    src/duckdb/duckdb_statement_batch_reader.cpp
    src/duckdb/duckdb_server.cpp
    src/duckdb/duckdb_tables_schema_batch_reader.cpp)

add_subdirectory(jwt-cpp)

# Add a library target
add_library(flight_sql_library
		src/flight_sql_library.cpp
		src/flight_sql_security.cpp
		${ARROW_FLIGHT_SQL_SQLITE_SERVER_SRCS}
		${ARROW_FLIGHT_SQL_DUCKDB_SERVER_SRCS}
)

# Set the output name for the library (libflight_sql_custom_name.a)
set_target_properties(flight_sql_library PROPERTIES OUTPUT_NAME flight_sql)

target_include_directories(flight_sql_library PRIVATE src/sqlite src/duckdb /usr/local/include)

target_link_libraries(flight_sql_library PRIVATE
		jwt-cpp
		arrow_shared
		arrow_flight_shared
		arrow_flight_sql_shared
		Threads::Threads
		${SQLite3_LIBRARIES}
		${DUCKDB}
		${Boost_LIBRARIES}
)

add_executable(flight_sql
    src/flight_sql.cpp
)

target_link_libraries(flight_sql PRIVATE
		flight_sql_library
		${Boost_LIBRARIES}
)

# Specify the installation targets
install(TARGETS flight_sql_library
		LIBRARY DESTINATION lib
		ARCHIVE DESTINATION lib
		RUNTIME DESTINATION bin
)
