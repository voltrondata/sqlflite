cmake_minimum_required(VERSION 3.25)
project(flight_sql)

set(CMAKE_CXX_STANDARD 17)

# Download and unpack arrow at configure time
configure_file(Arrow_CMakeLists.txt.in arrow/CMakeLists.txt)
execute_process(COMMAND "${CMAKE_COMMAND}" -G "${CMAKE_GENERATOR}" .
		WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/arrow"
)
execute_process(COMMAND "${CMAKE_COMMAND}" --build .
		WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/arrow"
)

find_package(Arrow REQUIRED PATHS ${CMAKE_BINARY_DIR}/3rd_party/arrow NO_DEFAULT_PATH)
find_package(ArrowFlight REQUIRED PATHS ${CMAKE_BINARY_DIR}/3rd_party/arrow NO_DEFAULT_PATH)
find_package(ArrowFlightSql REQUIRED PATHS ${CMAKE_BINARY_DIR}/3rd_party/arrow NO_DEFAULT_PATH)

# ---------------------

find_package(Threads REQUIRED)
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

# Add header file
set(HEADER_FILES src/library/include/flight_sql_library.h)

# Add a library target
add_library(flightsqlserver STATIC
		src/library/flight_sql_library.cpp
		src/library/flight_sql_security.cpp
		${ARROW_FLIGHT_SQL_SQLITE_SERVER_SRCS}
		${ARROW_FLIGHT_SQL_DUCKDB_SERVER_SRCS}
)

set_target_properties(flightsqlserver PROPERTIES PUBLIC_HEADER ${HEADER_FILES})

target_include_directories(flightsqlserver PRIVATE
		src/sqlite
		src/duckdb
		${ARROW_INCLUDE_DIR}
		/usr/local/include
)

target_link_libraries(flightsqlserver PRIVATE
		jwt-cpp
		Threads::Threads
		Arrow::arrow_static
		ArrowFlight::arrow_flight_static
		ArrowFlightSql::arrow_flight_sql_static
		${SQLite3_LIBRARIES}
		${DUCKDB}
		${Boost_LIBRARIES}
		"-lresolv"
)

if(APPLE)
	# macOS-specific libraries and options
	target_link_libraries(flightsqlserver PRIVATE "-framework CoreFoundation")
elseif (UNIX)
	target_link_libraries(flightsqlserver PRIVATE "-lssl -lcrypto")
endif()

# Specify the installation directory for the library
install(TARGETS flightsqlserver
		LIBRARY DESTINATION lib
		PUBLIC_HEADER DESTINATION include
)

# ------------ Executable section ------------
add_executable(flight_sql
    src/flight_sql.cpp
)

target_link_libraries(flight_sql PRIVATE
		flightsqlserver
		${Boost_LIBRARIES}
)

target_compile_options(flight_sql PRIVATE "-static")
