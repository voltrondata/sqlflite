#!/bin/bash

set -e
set -o pipefail

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

# Copy DuckDB executable and shared libraries/headers to /usr/local
pushd duckdb
cp build/release/duckdb /usr/local/bin
cp build/release/src/libduckdb* /usr/local/lib/
cp src/include/duckdb.h /usr/local/include/
cp src/include/duckdb.hpp /usr/local/include/
cp -R src/include/duckdb /usr/local/include/

popd
