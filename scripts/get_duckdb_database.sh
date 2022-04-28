#/bin/bash
TEMP_DIR="../temp"

# check if temp directory exists
if [ ! -d "$TEMP_DIR" ]; then
    echo "$TEMP_DIR doesn't exist. Creating."
fi

# move the database to temp
cd $TEMP_DIR

# check if duckdb database already exists
if [ -f "TPC-H-small.duckdb" ]; then
    rm TPC-H-small.duckdb
fi

# copy the SQLite db
cp ../data/TPC-H-small.db .

# move the data to DuckDB
$CONDA_PREFIX/bin/python ../scripts/move_data_to_duckdb.py 

# and push the new DB to data folder
cp TPC-H-small.duckdb ../data/