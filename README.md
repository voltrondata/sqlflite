# Arrow Flight SQL server - DuckDB / SQLite

[<img src="https://img.shields.io/badge/dockerhub-image-green.svg?logo=Docker">](https://hub.docker.com/r/voltrondata/flight-sql)
[<img src="https://img.shields.io/badge/Documentation-dev-yellow.svg?logo=">](https://arrow.apache.org/docs/format/FlightSql.html)
[<img src="https://img.shields.io/badge/GitHub-voltrondata%2Fflight--sql--server--example-blue.svg?logo=Github">](https://github.com/voltrondata/flight-sql-server-example)
[<img src="https://img.shields.io/badge/Arrow%20JDBC%20Driver-download%20artifact-red?logo=Apache%20Maven">](https://search.maven.org/search?q=a:flight-sql-jdbc-driver)
[<img src="https://img.shields.io/badge/PyPI-Arrow%20ADBC%20Flight%20SQL%20driver-blue?logo=PyPI">](https://pypi.org/project/adbc-driver-flightsql/)

## Description

This repo demonstrates how to build an Apache Arrow Flight SQL server implementation using DuckDB or SQLite as a backend database.

It enables authentication via middleware and allows for encrypted connections to the database via TLS.

For more information about Apache Arrow Flight SQL - please see this [article](https://voltrondata.com/resources/apache-arrow-flight-sql-arrow-for-every-database-developer). 

## Option 1 - Running from the published Docker image

Open a terminal, then pull and run the published Docker image which has everything setup (change: "--detach" to "--interactive" if you wish to see the stdout on your screen) - with command:

```bash
docker run --name flight-sql \
           --detach \
           --rm \
           --tty \
           --init \
           --publish 31337:31337 \
           --env TLS_ENABLED="1" \
           --env FLIGHT_PASSWORD="flight_password" \
           --env PRINT_QUERIES="1" \
           --pull missing \
           voltrondata/flight-sql:latest
```

The above command will automatically mount a very small TPC-H DuckDB database file.

**Note**: You can disable TLS in the container by setting environment variable: `TLS_ENABLED` to "0" (default is "1" - enabled).  This is not recommended unless you are using an mTLS sidecar in Kubernetes or something similar, as it will be insecure.    

### Optional - open a different database file
When running the Docker image - you can have it run your own DuckDB database file (the database must be built with DuckDB version: 0.9.2).   

Prerequisite: DuckDB CLI   
Install DuckDB CLI version [0.9.2](https://github.com/duckdb/duckdb/releases/tag/v0.9.2) - and make sure the executable is on your PATH.

Platform Downloads:   
[Linux x86-64](https://github.com/duckdb/duckdb/releases/download/v0.9.2/duckdb_cli-linux-amd64.zip)   
[Linux arm64 (aarch64)](https://github.com/duckdb/duckdb/releases/download/v0.9.2/duckdb_cli-linux-aarch64.zip)   
[MacOS Universal](https://github.com/duckdb/duckdb/releases/download/v0.9.2/duckdb_cli-osx-universal.zip)

In this example, we'll generate a new TPC-H Scale Factor 1 (1GB) database file, and then run the docker image to mount it:

```bash
# Generate a TPC-H database in the host's /tmp directory
pushd /tmp

duckdb ./tpch_sf1.duckdb << EOF
.bail on
.echo on
SELECT VERSION();
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=1);
EOF

# Run the flight-sql docker container image - and mount the host's DuckDB database file created above inside the container
docker run --name flight-sql \
           --detach \
           --rm \
           --tty \
           --init \
           --publish 31337:31337 \
           --env TLS_ENABLED="1" \
           --env FLIGHT_PASSWORD="flight_password" \
           --pull missing \
           --mount type=bind,source=$(pwd),target=/opt/flight_sql/data \
           --env DATABASE_FILENAME="data/tpch_sf1.duckdb" \
           voltrondata/flight-sql:latest
```

### Running initialization SQL commands
You can now run initialization commands upon container startup by setting environment variable: `INIT_SQL_COMMANDS` to a string of SQL commands separated by semicolons - example value:   

`SET threads = 1; SET memory_limit = '1GB';`.      

Here is a full example of running the Docker image with initialization SQL commands:
```bash
docker run --name flight-sql \
           --detach \
           --rm \
           --tty \
           --init \
           --publish 31337:31337 \
           --env TLS_ENABLED="1" \
           --env FLIGHT_PASSWORD="flight_password" \
           --env PRINT_QUERIES="1" \
           --env INIT_SQL_COMMANDS="SET threads = 1; SET memory_limit = '1GB';" \
           --pull missing \
           voltrondata/flight-sql:latest
```

You can also specify a file containing initialization SQL commands by setting environment variable: `INIT_SQL_COMMANDS_FILE` to the path of the file containing the SQL commands - example value: `/tmp/init.sql`.  The file must be mounted inside the container.   

**Note**: for the DuckDB back-end - the following init commands are automatically run for you:   
`SET autoinstall_known_extensions = true; SET autoload_known_extensions = true;`

**Note**: Initialization SQL commands which SELECT data will NOT show the results (this is not supported).

**Note**: Initialization SQL commands which fail will cause the Flight SQL server to abort and exit with a non-zero exit code. 

### Connecting to the server via JDBC
Download the [Apache Arrow Flight SQL JDBC driver](https://search.maven.org/search?q=a:flight-sql-jdbc-driver)

You can then use the JDBC driver to connect from your host computer to the locally running Docker Flight SQL server with this JDBC string (change the password value to match the value specified for the FLIGHT_PASSWORD environment variable if you changed it from the example above):
```bash
jdbc:arrow-flight-sql://localhost:31337?useEncryption=true&user=flight_username&password=flight_password&disableCertificateVerification=true
```

For instructions on setting up the JDBC driver in popular Database IDE tool: [DBeaver Community Edition](https://dbeaver.io) - see this [repo](https://github.com/voltrondata/setup-arrow-jdbc-driver-in-dbeaver).

**Note** - if you stop/restart the Flight SQL Docker container, and attempt to connect via JDBC with the same password - you could get error: "Invalid bearer token provided. Detail: Unauthenticated".  This is because the client JDBC driver caches the bearer token signed with the previous instance's RSA private key.  Just change the password in the new container by changing the "FLIGHT_PASSWORD" env var setting - and then use that to connect via JDBC.  

### Connecting to the server via the new [ADBC Python Flight SQL driver](https://pypi.org/project/adbc-driver-flightsql/)

You can now use the new Apache Arrow Python ADBC Flight SQL driver to query the Flight SQL server.  ADBC offers performance advantages over JDBC - because it minimizes serialization/deserialization, and data stays in columnar format at all phases.

You can learn more about ADBC and Flight SQL [here](https://voltrondata.com/resources/simplifying-database-connectivity-with-arrow-flight-sql-and-adbc).

Ensure you have Python 3.9+ installed, then open a terminal, then run:
```bash
# Create a Python virtual environment
python3 -m venv ./venv

# Activate the virtual environment
. ./venv/bin/activate

# Install the requirements including the new Arrow ADBC Flight SQL driver
pip install --upgrade pip
pip install pandas pyarrow adbc_driver_flightsql

# Start the python interactive shell
python
```

In the Python shell - you can then run:
```python
from adbc_driver_flightsql import dbapi as flight_sql, DatabaseOptions

flight_password = "flight_password" # Use an env var in production code!

with flight_sql.connect(uri="grpc+tls://localhost:31337",
                        db_kwargs={"username": "flight_username",
                                   "password": flight_password,
                                   DatabaseOptions.TLS_SKIP_VERIFY.value: "true" # Not needed if you use a trusted CA-signed TLS cert
                                   }
                        ) as conn:
   with conn.cursor() as cur:
       cur.execute("SELECT n_nationkey, n_name FROM nation WHERE n_nationkey = ?",
                   parameters=[24]
                   )
       x = cur.fetch_arrow_table()
       print(x)
```

You should see results:
```text
pyarrow.Table
n_nationkey: int32
n_name: string
----
n_nationkey: [[24]]
n_name: [["UNITED STATES"]]
```

### Tear-down
Stop the docker image with:
```bash
docker stop flight-sql
```

## Option 2 - Steps to build the solution manually

In order to run build the solution manually, and run SQLite and DuckDB Flight SQL server, you need to set up a new Python 3.8+ virtual environment on your machine. 
Follow these steps to do so (thanks to David Li!).

1. Clone the repo and build the static library and executable
```bash
git clone https://github.com/voltrondata/flight-sql-server-example --recurse-submodules
cd flight-sql-server-example

# Build and install the static library and executable
mkdir build
cd build
cmake -S .. -G Ninja -DCMAKE_INSTALL_PREFIX=/usr/local
cmake --build . --target install
```

2. Install Python requirements for ADBC client interaction - (ensure you have Python 3.9+ installed first)
```bash
python3 -m venv ./venv
. ./venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install --requirement ./requirements.txt
````

3. Get some SQLite3 sample data.
```bash
wget https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H-small.db -O ./data/TPC-H-small.sqlite
```

4. Create a DuckDB database.
```bash
python "scripts/create_duckdb_database_file.py" \
       --file-name="TPC-H-small.duckdb" \
       --file-path="data" \
       --overwrite-file=true \
       --scale-factor=0.01
```

5. Optionally generate TLS certificates for encrypting traffic to/from the Flight SQL server
```bash
pushd tls
./gen-certs.sh
popd
```

6. Start the Flight SQL server (and print client SQL commands as they run using the --print-queries option)
```bash
FLIGHT_PASSWORD="flight_password" flight_sql --database-filename data/TPC-H-small.duckdb --print-queries
```

## Selecting different backends
This option allows choosing from two backends: SQLite and DuckDB. It defaults to DuckDB.

```bash
$ FLIGHT_PASSWORD="flight_password" flight_sql --database-filename data/TPC-H-small.duckdb
Apache Arrow version: 15.0.0
WARNING - TLS is disabled for the Flight SQL server - this is insecure.
DuckDB version: v0.9.2
Running Init SQL command: 
SET autoinstall_known_extensions = true;
Running Init SQL command: 
 SET autoload_known_extensions = true;
Using database file: "/opt/flight_sql/data/TPC-H-small.duckdb"
Print Queries option is set to: false
Apache Arrow Flight SQL server - with engine: DuckDB - will listen on grpc+tcp://0.0.0.0:31337
Flight SQL server - started
```

The above call is equivalent to running `flight_sql -B duckdb` or `flight_sql --backend duckdb`. To select SQLite run

```bash
FLIGHT_PASSWORD="flight_password" flight_sql -B sqlite -D data/TPC-H-small.sqlite 
```
or 
```bash
FLIGHT_PASSWORD="flight_password" flight_sql --backend sqlite --database-filename data/TPC-H-small.sqlite
```
The above will produce the following:

```bash
Apache Arrow version: 15.0.0
WARNING - TLS is disabled for the Flight SQL server - this is insecure.
SQLite version: 3.45.0
Using database file: "/opt/flight_sql/data/TPC-H-small.sqlite"
Print Queries option is set to: false
Apache Arrow Flight SQL server - with engine: SQLite - will listen on grpc+tcp://0.0.0.0:31337
Flight SQL server - started
```

## Print help
To see all the available options run `flight_sql --help`.

```bash
flight_sql --help
Allowed options:
  --help                              produce this help message
  -B [ --backend ] arg (=duckdb)      Specify the database backend. Allowed 
                                      options: duckdb, sqlite.
  -H [ --hostname ] arg               Specify the hostname to listen on for the
                                      Flight SQL Server.  If not set, we will 
                                      use env var: 'FLIGHT_HOSTNAME'.  If that 
                                      isn't set, we will use the default of: 
                                      '0.0.0.0'.
  -R [ --port ] arg (=31337)          Specify the port to listen on for the 
                                      Flight SQL Server.
  -D [ --database-filename ] arg      Specify the database filename (absolute 
                                      or relative to the current working 
                                      directory)
  -U [ --username ] arg               Specify the username to allow to connect 
                                      to the Flight SQL Server for clients.  If
                                      not set, we will use env var: 
                                      'FLIGHT_USERNAME'.  If that isn't set, we
                                      will use the default of: 
                                      'flight_username'.
  -P [ --password ] arg               Specify the password to set on the Flight
                                      SQL Server for clients to connect with.  
                                      If not set, we will use env var: 
                                      'FLIGHT_PASSWORD'.  If that isn't set, 
                                      the server will exit with failure.
  -S [ --secret-key ] arg             Specify the secret key used to sign JWTs 
                                      issued by the Flight SQL Server. If it 
                                      isn't set, we use env var: 'SECRET_KEY'. 
                                      If that isn't set, the server will create
                                      a random secret key.
  -T [ --tls ] arg                    Specify the TLS certificate and key file 
                                      paths.
  -I [ --init-sql-commands ] arg      Specify the SQL commands to run on server
                                      startup.  If not set, we will use env 
                                      var: 'INIT_SQL_COMMANDS'.
  -F [ --init-sql-commands-file ] arg Specify a file containing SQL commands to
                                      run on server startup.  If not set, we 
                                      will use env var: 'INIT_SQL_COMMANDS_FILE
                                      '.
  -M [ --mtls-ca-cert-filename ] arg  Specify an optional mTLS CA certificate 
                                      path used to verify clients.  The 
                                      certificate MUST be in PEM format.
  -Q [ --print-queries ]              Print queries run by clients to stdout
```
