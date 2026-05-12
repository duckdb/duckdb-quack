# DuckDB Quack Client/Server Bulk Benchmark, 2026-05-10, hannes
import os
import time
import math
import statistics
import enum

Mode = enum.Enum('Mode', ['QUACK', 'POSTGRES', 'ARROW'])

# change this as desired
server_hostname = 'localhost'
benchmark_mode = Mode.QUACK

install_duckdb_extensions = False

def connect(mode):
    global install_duckdb_extensions
    match mode:
        case Mode.QUACK:
            import duckdb
            con = duckdb.connect()
            if install_duckdb_extensions:
                con.execute(f"""
FORCE INSTALL httpfs; 
FORCE INSTALl quack FROM core_nightly; 
""")
                install_duckdb_extensions = False

            con.execute(f"""
LOAD httpfs;
LOAD quack;
""")
        case Mode.POSTGRES:
            import psycopg2
            con = psycopg2.connect(f'postgresql://{server_hostname}')
            con.autocommit = True

        case Mode.ARROW:
            from adbc_driver_flightsql import dbapi
            con = dbapi.connect(
                uri=f'grpc://{server_hostname}:31337',
                db_kwargs={"username": "gizmosql_user",
                         "password": "gizmosql_password"},
                autocommit=True)
    return con


def execute(mode, cursor, sql):
    match mode:
        case Mode.QUACK:
            cursor.execute(f"EXPLAIN ANALYZE FROM quack_query('quack:{server_hostname}', '{sql}', token='asdf')")

        case Mode.POSTGRES:
            cursor.execute(sql)

        case Mode.ARROW:
            cursor.execute(sql)

print('method\trows\tmedian_time_seconds')

for rows in [int(math.pow(10,y)) for y in range(2, 8)] + [59986052]:
	timings = []
	for rep in range(5):
		con = connect(benchmark_mode)
		cursor = con.cursor()

		start = time.time()
		execute(benchmark_mode, cursor, f'SELECT * FROM lineitem LIMIT {rows}')

		timings.append(time.time() - start)
		cursor.close()
		con.close()

	median_time = round(statistics.median(timings), 3)
	print(f'{benchmark_mode}\t{rows}\t{median_time}')

