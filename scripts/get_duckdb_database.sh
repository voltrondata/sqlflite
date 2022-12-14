#!/bin/bash

set -e
set -o pipefail

SCRIPT_DIR=$(dirname ${0})

mkdir -p "${SCRIPT_DIR}/../data"

pushd "${SCRIPT_DIR}/../data"
rm -f TPC-H-small.duckdb
popd

# move the data to DuckDB
python "${SCRIPT_DIR}/move_data_to_duckdb.py"
