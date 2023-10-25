#!/bin/bash

SCRIPT_DIR=$(dirname ${0})
TLS_DIR=${SCRIPT_DIR}/../tls

# Set a dummy password for the test...
export FLIGHT_PASSWORD="testing123"

# Start the Flight SQL Server - in the background...
${SCRIPT_DIR}/start_flight_sql.sh &

# Sleep for a few seconds to allow the server to have time for initialization...
sleep 20

python "${SCRIPT_DIR}/test_flight_sql.py"

RC=$?

# Stop the server...
kill %1

popd

# Remove temporary TLS cert files
pushd ${TLS_DIR}
rm -f ./*.csr \
      ./*.key \
      ./*.pkcs1 \
      ./*.pem \
      ./*.srl

popd

# Exit with the code of the python test...
exit ${RC}
