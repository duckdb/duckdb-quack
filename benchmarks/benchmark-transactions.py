# DuckDB Quack Client/Server Transaction Benchmark, 2026-05-10, hannes

import datetime 
import enum
import math
import os
import random
import statistics 
import string
import sys
import threading
import time


Mode = enum.Enum('Mode', ['QUACK', 'POSTGRES', 'ARROW'])

# change this as desired
server_hostname = 'localhost'
benchmark_mode = Mode.QUACK


# orrrr
class ReturningThread(threading.Thread):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}, verbose=None):
        super().__init__(group, target, name, args, kwargs)
        self._return = None

    def run(self):
        self._return = self._target(*self._args, **self._kwargs)

    def join(self):
        super().join()
        return self._return

install_duckdb_extensions = True

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
ATTACH 'quack:{server_hostname}' as db (DISABLE_SSL true, token 'asdf'); 
PRAGMA threads=1;
""")
        case Mode.POSTGRES:
            import psycopg2
            con = psycopg2.connect(f'postgresql://{server_hostname}', sslmode='disable')
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
            sql2 = sql.replace("'", "''")
            cursor.execute(f"CALL db.query('{sql2}');")

        case Mode.POSTGRES:
            cursor.execute(sql)

        case Mode.ARROW:
            cursor.execute(sql)


def thread_fun(id):
    con = connect(benchmark_mode)
    cursor = con.cursor()

    transactions_completed = 0
    transactions_failed = 0
    start_date = datetime.datetime.strptime('1992-01-01', '%Y-%m-%d')

    while not stop:
        # generate lineitem-ish data, just so messages have some weight
        l_orderkey = random.randint(1, 60000000)
        l_partkey = random.randint(1, 100000)
        l_suppkey = random.randint(1, 100000)
        l_linenumber = random.randint(1, 7)
        l_quantity = round(random.randint(100, 5000)/100, 2)
        l_extendedprice = round(random.randint(900,  10000000 )/100, 2)
        l_discount = round(random.randint(1,  100 ) / 10, 2)
        l_tax = round(random.randint(1,  100 ) / 10, 2)
        l_returnflag = random.choice(string.ascii_uppercase)
        l_linestatus = random.choice(string.ascii_uppercase)
        l_shipdate = start_date + datetime.timedelta(days=random.randint(0, 3000))
        l_commitdate = start_date + datetime.timedelta(days=random.randint(0, 3000))
        l_receiptdate = start_date + datetime.timedelta(days=random.randint(0, 3000))
        l_shipinstruct = ''.join(random.choices(string.ascii_letters, k=random.randint(10, 20)))
        l_shipmode = ''.join(random.choices(string.ascii_letters, k=random.randint(5, 10)))
        l_comment = ''.join(random.choices(string.ascii_letters, k=random.randint(10, 30)))

        sql = f'''
INSERT INTO lineitem_insert VALUES ({l_orderkey}, {l_partkey}, {l_suppkey}, {l_linenumber}, {l_quantity}, {l_extendedprice}, {l_discount}, {l_tax}, '{l_returnflag}','{l_linestatus}','{l_shipdate}','{l_commitdate}', '{l_receiptdate}', '{l_shipinstruct}', '{l_shipmode}', '{l_comment}');
'''
        execute(benchmark_mode, cursor, sql)
        transactions_completed +=1

    cursor.close()
    con.close()
    return transactions_completed

create_sql = '''
DROP TABLE IF EXISTS lineitem_insert;
CREATE TABLE lineitem_insert(l_orderkey BIGINT NOT NULL, l_partkey BIGINT NOT NULL, l_suppkey BIGINT NOT NULL, l_linenumber BIGINT NOT NULL, l_quantity DECIMAL(15,2) NOT NULL, l_extendedprice DECIMAL(15,2) NOT NULL, l_discount DECIMAL(15,2) NOT NULL, l_tax DECIMAL(15,2) NOT NULL, l_returnflag VARCHAR NOT NULL, l_linestatus VARCHAR NOT NULL, l_shipdate DATE NOT NULL, l_commitdate DATE NOT NULL, l_receiptdate DATE NOT NULL, l_shipinstruct VARCHAR NOT NULL, l_shipmode VARCHAR NOT NULL, l_comment VARCHAR NOT NULL);
        '''

duration = 5
for n_threads in [1 ,2, 4 ,8, 16, 32, 64]:
    results = []
    for rep in range(5):
        con = connect(benchmark_mode)
        cursor = con.cursor()
        execute(benchmark_mode, cursor, create_sql)
        cursor.close()
        con.close()

        start = time.time()
        stop = False

        threads = []
        for i in range(n_threads):
            t = ReturningThread(target=thread_fun, args=[i])
            t.start()
            threads.append(t)

        time.sleep(duration)
        stop = True
        total_transactions_completed = 0

        for t in threads:
            res = t.join()
            total_transactions_completed += res

        total_time = time.time() - start
        transactions_per_second = total_transactions_completed/total_time
        results.append(transactions_per_second)

    median_tps = round(statistics.median(results), 3)
    print(f'{benchmark_mode}\t{n_threads}\t\t{median_tps}')
