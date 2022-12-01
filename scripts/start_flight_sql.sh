#!/bin/bash

L_DATABASE_BACKEND=${1:-${DATABASE_BACKEND:-"duckdb"}}
L_DATABASE_FILE_PATH=${2:-${DATABASE_FILE_PATH:-"../data"}}
L_DATABASE_FILE_NAME=${3:-${DATABASE_FILE_NAME:-"TPC-H-small.duckdb"}}

# Generate TLS certificates if they are not present...
pushd ../tls
if [ ! -f ./cert0.pem ]
then
   echo -n "Generating TLS certs...\n"
   ./gen-certs.sh
fi
popd

pushd ../build
./flight_sql --backend="${L_DATABASE_BACKEND}" --database_file_path="${L_DATABASE_FILE_PATH}" --database_file_name="${L_DATABASE_FILE_NAME}"
