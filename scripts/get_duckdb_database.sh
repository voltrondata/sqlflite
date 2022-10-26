#!/bin/bash

set -e
set -o pipefail

pushd data
# check if duckdb database already exists
if [ -f "TPC-H-small.duckdb" ]; then
    rm TPC-H-small.duckdb
fi
popd

# move the data to DuckDB
python scripts/move_data_to_duckdb.py
