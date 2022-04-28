# Arrow FlightSQL example

## Setup

In order to run this example you need to set up a new environment on your machine. 
Follow these steps to do so (thanks to David Li!).

1. Make sure you have [miniconda](https://docs.conda.io/en/latest/miniconda.html) installed on your machine. 
2. Install Mamba: `conda install -c conda-forge mamba`.
3. Now, create a new dev environment
```bash
$ mamba create -n flight-sql -c conda-forge python=3.9
$ conda activate flight-sql
$ 
```
4. Build and install Arrow
```bash
$ cd scripts && ./install_arrow.sh
```
7. Build and install `duckdb`. This is sometimes necessary as conda `compilers` 
seem to be including incompatible GlibC library with the compiled binaries
of `duckdb`.
```bash
$ ./install_duckdb.sh
```
6. Get the data.
```bash
$ mkdir ../data
$ wget https://github.com/lovasoa/TPCH-sqlite/releases/download/v1.0/TPC-H-small.db -O ../data/TPC-H-small.db
```
7. Create duckdb database.
```bash
$ ./get_duckdb_database.sh
```
9. Build the example.
```bash
$ mkdir build && cd build
$ cmake .. -GNinja -DCMAKE_PREFIX_PATH=$CONDA_PREFIX/lib/cmake/arrow
$ ninja && ./flight_sql
```