#!/bin/bash

SCRIPT_DIR=$(dirname ${0})
TLS_DIR=${SCRIPT_DIR}/../tls

L_DATABASE_BACKEND=${1:-${DATABASE_BACKEND:-"duckdb"}}
L_DATABASE_FILENAME=${3:-${DATABASE_FILENAME:-"data/TPC-H-small.duckdb"}}
L_PRINT_QUERIES=${4:-${PRINT_QUERIES:-"1"}}

# Setup the print_queries option
PRINT_QUERIES_FLAG=""
if [ "${L_PRINT_QUERIES}" == "1" ]
then
  PRINT_QUERIES_FLAG="--print-queries"
fi

# Generate TLS certificates if they are not present...
pushd ${TLS_DIR}
if [ ! -f ./cert0.pem ]
then
   echo -n "Generating TLS certs...\n"
   ./gen-certs.sh
fi
popd

flight_sql --backend="${L_DATABASE_BACKEND}" --database-filename="${L_DATABASE_FILENAME}" ${PRINT_QUERIES_FLAG}
