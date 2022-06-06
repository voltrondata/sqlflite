#!/bin/bash

set -e
set -o pipefail

# Ensure that CONDA_PREFIX is set
echo "Environment variable: CONDA_PREFIX=${CONDA_PREFIX:?is not set - aborting!}"
echo "Environment variable: DUCKDB_COMMIT_HASH=${DUCKDB_COMMIT_HASH:-e9957d10ff11d32feb78f1601109a3c8a9f0578f}"

TEMP_DIR="../temp"

# check if temp directory exists
if [ ! -d "$TEMP_DIR" ]; then
    echo "$TEMP_DIR doesn't exist. Creating."
    mkdir "$TEMP_DIR"
fi

# clone the repository
cd $TEMP_DIR
if [ ! -d "duckdb" ]; then
    echo "Cloning DuckDB."
    git clone https://github.com/duckdb/duckdb.git
fi

cd duckdb
if [ ! -d "build/release" ]; then
    git reset --hard ${DUCKDB_COMMIT_HASH}
    echo "Building DuckDB"
    make
fi

# copy libraries to include and lib paths
cp build/release/src/libduckdb.so $CONDA_PREFIX/lib/
cp src/include/duckdb.h $CONDA_PREFIX/include/
cp src/include/duckdb.hpp $CONDA_PREFIX/include/
cp -R src/include/duckdb $CONDA_PREFIX/include/