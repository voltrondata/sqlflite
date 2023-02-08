#!/bin/bash

SCRIPT_DIR=$(dirname ${0})

L_DATABASE_BACKEND=${1:-${DATABASE_BACKEND:-"duckdb"}}
L_DATABASE_FILE_PATH=${2:-${DATABASE_FILE_PATH:-"${SCRIPT_DIR}/../data"}}
L_DATABASE_FILE_NAME=${3:-${DATABASE_FILE_NAME:-"TPC-H-small.duckdb"}}
L_PRINT_QUERIES=${4:-${PRINT_QUERIES:-"1"}}

# Setup the print_queries option
PRINT_QUERIES_FLAG=""
if [ "${PRINT_QUERIES}" == "1" ]
then
  PRINT_QUERIES_FLAG="--print_queries"
fi

# Generate TLS certificates if they are not present...
pushd "${SCRIPT_DIR}/../tls"
if [ ! -f ./cert0.pem ]
then
   echo -n "Generating TLS certs...\n"
   ./gen-certs.sh
fi
popd

pushd "${SCRIPT_DIR}/../build"
./flight_sql --backend="${L_DATABASE_BACKEND}" --database_file_path="${L_DATABASE_FILE_PATH}" --database_file_name="${L_DATABASE_FILE_NAME}" ${PRINT_QUERIES_FLAG}
