#!/bin/bash

# Set a dummy password for the test...
export FLIGHT_PASSWORD="testing123"

# Start the Flight SQL Server - in the background...
./start_flight_sql.sh &

# Sleep for a few seconds to allow the server to have time for initialization...
sleep 5

python "test_flight_sql.py"

RC=$?

# Stop the server...
kill %1

# Remove temporary TLS cert files
rm -f ../tls/*.csr \
      ../tls/*.key \
      ../tls/*.pkcs1 \
      ../tls/*.pem \
      ../tls/*.srl

# Exit with the code of the python test...
exit ${RC}
