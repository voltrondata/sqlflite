#!/bin/bash
# check if temp dir exists
DIRECTORY="temp"
if [[ ! -d "$DIRECTORY" ]]
then
    echo "Directory $DIRECTORY does not exist on your filesystem. Adding..."
    mkdir $DIRECTORY
fi

# download the latest release of DuckDB
cd $DIRECTORY

# Do some Mac stuff if needed...
OS=$(uname)
if [ "${OS}" == "Darwin" ]; then
  echo "Downloading Mac-specific DuckDB..."
  DUCKDB_PAYLOAD="https://github.com/duckdb/duckdb/releases/download/v0.6.1/libduckdb-osx-universal.zip"
elif [ "$OS" == "Linux" ]; then
  echo "Downloading Linux-specific DuckDB..."
  DUCKDB_PAYLOAD="https://github.com/duckdb/duckdb/releases/download/v0.6.1/libduckdb-linux-amd64.zip"
fi

FNAME=$(echo $DUCKDB_PAYLOAD | grep -oE "[^/]+$")
if [[ ! -f "$FNAME" ]]; then
  wget $DUCKDB_PAYLOAD
else
  echo "Already downloaded"
fi

# unzip 
if [[ ! -f "duckdb.hpp" ]]; then 
  unzip $FNAME
fi

# copy to PATH
cp duckdb.h* "$CONDA_PREFIX/include/"
cp libduckdb.so "$CONDA_PREFIX/lib/"