#!/bin/bash

set -e
set -o pipefail

mkdir -p data

pushd data
# check if duckdb database already exists
if [ -f "TPC-H-small.duckdb" ]; then
    rm TPC-H-small.duckdb
fi
popd

SCRIPT_DIR=$(dirname ${0})

# move the data to DuckDB
python ${SCRIPT_DIR}/move_data_to_duckdb.py
