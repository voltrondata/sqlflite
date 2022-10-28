#!/bin/bash

set -e
set -o pipefail

DUCKDB_VERSION=${DUCKDB_VERSION:-"v0.5.1"}
echo "Variable: DUCKDB_VERSION=${DUCKDB_VERSION}"

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

# Build the python library from source
pushd duckdb/tools/pythonpkg
python setup.py install
popd

# Copy DuckDB shared libraries/headers to /usr/local
pushd duckdb
cp build/release/src/libduckdb* /usr/local/lib/
cp src/include/duckdb.h /usr/local/include/
cp src/include/duckdb.hpp /usr/local/include/
cp -R src/include/duckdb /usr/local/include/
popd
