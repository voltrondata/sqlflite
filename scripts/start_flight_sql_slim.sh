#!/bin/bash

set -e

L_DATABASE_FILENAME=${1:-${DATABASE_FILENAME?"You must specify a database filename."}}
L_DATABASE_BACKEND=${2:-${DATABASE_BACKEND:-"duckdb"}}
L_PRINT_QUERIES=${3:-${PRINT_QUERIES:-"1"}}
L_TLS_ENABLED=${4:-${TLS_ENABLED:-"0"}}
L_TLS_CERT=${5:-${TLS_CERT:-"tls/cert0.pem"}}
L_TLS_KEY=${6:-${TLS_KEY:-"tls/cert0.key"}}

TLS_ARG=""
if [ "${L_TLS_ENABLED}" == "1" ]
then
  TLS_ARG="--tls ${L_TLS_CERT} ${L_TLS_KEY}"
  popd
fi

# Setup the print_queries option
PRINT_QUERIES_FLAG=""
if [ "${L_PRINT_QUERIES}" == "1" ]
then
  PRINT_QUERIES_FLAG="--print-queries"
fi

flight_sql_server --backend="${L_DATABASE_BACKEND}" --database-filename="${L_DATABASE_FILENAME}" ${TLS_ARG} ${PRINT_QUERIES_FLAG}
