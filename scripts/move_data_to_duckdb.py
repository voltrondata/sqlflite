import pandas as pd
import sqlite3
import duckdb
import os
from pathlib import Path


dir_path = os.path.dirname(os.path.realpath(__file__))

data_dir_path = Path(dir_path) / ".." / "data"
sqlite_db_file = data_dir_path / "TPC-H-small.db"
duckdb_db_file = data_dir_path / "TPC-H-small.duckdb"

# establish all connections to databases
con = sqlite3.connect(sqlite_db_file.as_posix())
con_duck = duckdb.connect(database=duckdb_db_file.as_posix(), read_only=False)

cur = con.cursor()

# get a list of all the tables
cur.execute('SELECT name from sqlite_master where type= "table"')
tables = cur.fetchall()

# get all the data
sqlite_data = {}

for table in tables: 
    tname = table[0]
    cur.execute(f'PRAGMA table_info({tname});')
    sqlite_data[tname] = (pd.read_sql(f'SELECT * FROM {tname}', con=con), cur.fetchall())

# insert the data into duckdb database
for table in tables:
    tname = table[0]

    temp_table = sqlite_data[tname][0]
    date_cols = [e[1] for e in sqlite_data[tname][1] if e[2] == 'DATE']

    for col in date_cols:
        temp_table[col] = pd.to_datetime(temp_table[col])

    con_duck.execute(f'CREATE TABLE {tname} AS SELECT * FROM temp_table')
    con_duck.execute(f'INSERT INTO {tname} SELECT * FROM temp_table')

# close the connections
con_duck.close()
con.close()

print(f"Successfully created DuckDB database file: {duckdb_db_file.as_posix()} "
      f"from SQLite3 database file: {sqlite_db_file.as_posix()}"
      )
