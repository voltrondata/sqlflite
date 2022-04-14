#include <iostream>
#include <duckdb.hpp>
#include <sqlite3.h>

int main(int argc, char *argv[]) {
    std::cout << "Flight SQL Example of running queries against "
                 "SQLite and DuckDB." << std::endl;

    duckdb::DuckDB db = duckdb::DuckDB(nullptr);
    duckdb::Connection con = duckdb::Connection(db);

    return 0;
}