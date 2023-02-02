# Arrow Flight SQL server - DuckDB / SQLite

## Option 1 - Running from the published Docker image

Open a terminal, then pull and run the published Docker image which has everything setup (change: "--detach" to "--interactive" if you wish to see the stdout on your screen) - with command:

```bash
docker run --name flight-sql \
           --detach \
           --rm \
           --tty \
           --init \
           --publish 31337:31337 \
           --env FLIGHT_PASSWORD="flight_password" \
           --pull missing \
           prmoorevoltron/flight-sql:latest
```

The above command will automatically mount a very small TPC-H DuckDB database file.

### Optional - open a different database file
When running the Docker image - you can have it run your own DuckDB database file (the database must be built with DuckDB version: 0.6.1).   

Prerequisite: DuckDB CLI   
Install DuckDB CLI version [0.6.1](https://github.com/duckdb/duckdb/releases/tag/v0.6.1) - and make sure the executable is on your PATH.

Platform Downloads:   
[Linux x86-64](https://github.com/duckdb/duckdb/releases/download/v0.6.1/duckdb_cli-linux-amd64.zip)   
[Linux arm64 (aarch64)](https://github.com/duckdb/duckdb/releases/download/v0.6.1/duckdb_cli-linux-aarch64.zip)   
[MacOS Universal](https://github.com/duckdb/duckdb/releases/download/v0.6.1/duckdb_cli-osx-universal.zip)

In this example, we'll generate a new TPC-H Scale Factor 1 (1GB) database file, and then run the docker image to mount it:

```bash
# Generate a TPC-H database in the host's /tmp directory
duckdb /tmp/tpch_sf1.duckdb << EOF
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
           --env FLIGHT_PASSWORD="flight_password" \
           --pull missing \
           --mount type=bind,source=/tmp,target=/opt/flight_sql/data \
           --env DATABASE_FILE_NAME="ibis_testing.duckdb" \
           prmoorevoltron/flight-sql:latest
```

### Connecting to the server via JDBC
Download the [Apache Arrow Flight SQL JDBC driver](https://search.maven.org/search?q=a:flight-sql-jdbc-driver)

You can then use the JDBC driver to connect from your host computer to the locally running Docker Flight SQL server with this JDBC string (change the password value to match the value specified for the FLIGHT_PASSWORD environment variable if you changed it from the example above):
```bash
jdbc:arrow-flight-sql://localhost:31337?useEncryption=true&user=flight_username&password=flight_password&disableCertificateVerification=true
```

Note - if you stop/restart the Flight SQL Docker container, and attempt to connect via JDBC with the same password - you could get error: "Invalid bearer token provided. Detail: Unauthenticated".  This is because the client JDBC driver caches the bearer token signed with the previous instance's RSA private key.  Just change the password in the new container by changing the "FLIGHT_PASSWORD" env var setting - and then use that to connect via JDBC.  

### Tear-down
Stop the docker image with:
```bash
docker kill flight-sql
```

## Option 2 - Steps to build the solution manually

In order to run build the solution manually, and run SQLite and DuckDB Flight SQL server, you need to set up a new Python 3.8+ virtual environment on your machine. 
Follow these steps to do so (thanks to David Li!).

1. Ensure you have Python 3.8+ installed, then create a virtual environment from the root of this repo and install requirements...
```bash
python3 -m venv ./venv
. ./venv/bin/activate
pip install --upgrade pip
pip install --requirement ./requirements.txt
```

2. Build and install Arrow
```bash
scripts/build_arrow.sh
```

3. Build and install `duckdb`. This is sometimes necessary as conda `compilers` 
seem to be including incompatible GlibC library with the compiled binaries
of `duckdb`.
```bash
scripts/build_duckdb.sh
```

4. Get some SQLite3 sample data.
```bash
wget https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H-small.db -O ./data/TPC-H-small.db
```

5. Create a DuckDB database.
```bash
python "scripts/create_duckdb_database_file.py" \
       --file-name="TPC-H-small.duckdb" \
       --file-path="data" \
       --overwrite-file=true \
       --scale-factor=0.01
```

6. Build the Flight SQL Server executable.
```bash
. ~/.bashrc
mkdir -p build
pushd build
cmake .. -GNinja -DCMAKE_PREFIX_PATH=$ARROW_HOME/lib/cmake
ninja
popd
```

7. Generate TLS certificates for encrypting traffic to/from the Flight SQL server
```bash
pushd tls
./gen-certs.sh
popd
```

## Docker
You can build a Docker container which performs all of the setup steps above.   

### To build on an x86-based machine:
```bash
docker build . --build-arg BUILD_PLATFORM="linux/amd64" --tag=flight_sql_amd64:latest

# Then run the container with:
docker run --name flight-sql \
           --rm \
           --interactive \
           --tty \
           --init \
           --publish 31337:31337 \
           --env FLIGHT_PASSWORD="flight_password" \
           flight_sql_amd64:latest
```

### To build on an M1 (ARM)-based Mac:
```bash
docker build . --build-arg BUILD_PLATFORM="linux/arm64" --tag=flight_sql_arm64:latest

# Then run the container with:
docker run --name flight-sql \
           --rm \
           --interactive \
           --tty \
           --init \
           --publish 31337:31337 \
           --env FLIGHT_PASSWORD="flight_password" \
           flight_sql_arm64:latest
```

### Connecting to the server via JDBC
Download the [Apache Arrow Flight SQL JDBC driver](https://search.maven.org/search?q=a:flight-sql-jdbc-driver)   

You can then use the JDBC driver to connect to a locally running Flight SQL server with this JDBC string (change the password value to match the value specified for the FLIGHT_PASSWORD environment variable if you changed it from the example above):
```bash
jdbc:arrow-flight-sql://localhost:31337?useEncryption=true&user=flight_username&password=flight_password&disableCertificateVerification=true
```


## Selecting different backends
This sqlite allows choosing from two backends: SQLite and DuckDB. It defaults to DuckDB.

```bash
$ FLIGHT_PASSWORD="flight_password" ./flight_sql --database_file_name "TPC-H-small.duckdb"
> Using database file: ../data/TPC-H-small.duckdb
> duckdb server listening on grpc+tls://0.0.0.0:31337
```

The above call is equivalent to running `./flight_sql -B duckdb` or `./flight_sql --backend duckdb`. To select SQLite run

```bash
FLIGHT_PASSWORD="flight_password" ./flight_sql -B sqlite -D "TPC-H-small.db" 
```
or 
```bash
FLIGHT_PASSWORD="flight_password" ./flight_sql --backend sqlite --database_file_name "TPC-H-small.db"
```
The above will produce the following:

```bash
> Using database file: ../data/TPC-H-small.db
> sqlite server listening on grpc+tls://0.0.0.0:31337
```

## Print help
To see all the available options run `./flight.sql --help`.

```bash
./flight_sql --help
Allowed options:
  --help                                produce this help message
  -B [ --backend ] arg (=duckdb)        Specify the database backend. Allowed 
                                        options: duckdb, sqlite.
  -P [ --database_file_path ] arg (=../data)
                                        Specify the search path for the 
                                        database file.
  -D [ --database_file_name ] arg       Specify the database filename (the file
                                        must be in search path)
```
