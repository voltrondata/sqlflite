import pandas as pd
import sqlite3
import duckdb

# establish all connections to databases
con = sqlite3.connect('TPC-H-small.db')
con_duck = duckdb.connect(database='TPC-H-small.duckdb', read_only=False)

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