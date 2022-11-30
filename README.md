# Arrow FlightSQL sqlite

## Setup

In order to run this SQLite and DuckDB Flight SQL server, you need to set up a new Python 3.8+ virtual environment on your machine. 
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

4. Get the data.
```bash
mkdir data
wget https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H-small.db -O ./data/TPC-H-small.db
```

5. Create duckdb database.
```bash
pushd scripts
get_duckdb_database.sh
popd
```

6. Build the Flight SQL Server executable.
```bash
. ~/.bashrc
mkdir build
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
docker run --interactive \
           --tty \
           --init \
           --publish 31337:31337 \
           flight_sql_amd64:latest
```

### To build on an M1 (ARM)-based Mac:
```bash
docker build . --build-arg BUILD_PLATFORM="linux/arm64" --tag=flight_sql_arm64:latest

# Then run the container with:
docker run --interactive \
           --tty \
           --init \
           --publish 31337:31337 \
           flight_sql_arm64:latest
```

## Selecting different backends
This sqlite allows chosing from two backends: SQLite and DuckDB. It defaults to DuckDB.

```bash
$ ./flight_sql
> duckdb server listening on localhost:31337
> Connected to server: localhost:31337
> Client created.
> ...
```

The above call is equivalent to running `./flight_sql -B duckdb` or `./flight_sql --backend duckdb`. To select SQLite run

```bash
./flight_sql -B sqlite
```
or 
```bash
./flight_sql --backend sqlite
```
The above will produce the following:

```bash
> sqlite server listening on localhost:31337
> Connected to server: localhost:31337
> Client created.
> ...
```

## Printing results of running query
This sqlite defaults to running the queries but does not print out
the query itself nor does it print the results. To switch to printing
results and queries set the `print` flag to `true`.

```bash
./flight_sql --print true
```

## Print help
To see all the available options run `./flight.sql --help`.

```bash
> Allowed options:
>   --help                         produce this help message
>   -B [ --backend ] arg (=duckdb) Specify the database backend. Allowed options:
>                                  duckdb, sqlite.
>   --print arg (=false)           Print the results of running queries. Allowed 
>                                  options: false, true.
```
