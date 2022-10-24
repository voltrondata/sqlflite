#!/bin/bash

set -e
set -o pipefail

echo "Environment variable: DUCKDB_VERSION=${DUCKDB_VERSION:-"v0.5.1"}"

if [ ! -d "duckdb" ]; then
    echo "Cloning DuckDB."
    git clone --depth 1 https://github.com/duckdb/duckdb.git --branch ${DUCKDB_VERSION}
fi

pushd duckdb
if [ ! -d "build/release" ]; then
    echo "Building DuckDB"
    make
fi
popd

# Install the python library from source
pushd duckdb/tools/pythonpkg
python setup.py install
popd
