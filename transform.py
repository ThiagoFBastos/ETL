#!/usr/bin/env python3

import pandas as pd
import os
import psycopg2
from sqlalchemy import create_engine

#dbconnect se conecta ao postgres local com o database northwind_output
def dbconnect(_database, _user, _password, _port):
    try:
        con = psycopg2.connect(host = 'localhost', port = _port, database = _database, user = _user, password = _password)
        return con
    except Exception as ex:
        print(f'erro ao se conectar com o database: {ex}')
        exit(0)

#dbexecute realiza um comando sql
def dbexecute(con, sql):
    cur = con.cursor()
    cur.execute(sql)

"""
    extrai os dados dos arquivos csv armazenados no disco
    e os insere nas tabelas do database
"""
def transform(data, database, user, password, port):

    try:
        engine = create_engine(f'postgresql+psycopg2://{user}:{password}@localhost:{port}/{database}')
    except Exception as ex:
        print(f'erro ao se conectar com o database local: {ex}')
        exit(0)

    tables = ['categories',
         'suppliers',
         'products',
         'employees',
         'region',
         'territories',
         'employee_territories',
         'customers',
         'shippers',
         'orders',
         'us_states',
         'customer_demographics',
         'customer_customer_demo'
    ]

    with dbconnect(database, user, password, port) as con:
        for table in reversed(tables):
            try:
                dbexecute(con, f'delete from {table}')
            except Exception as ex:
                print(f'erro ao deletar linhas da tabela {table}: {ex}')

    for table in tables:
        try:
            filepath = os.path.join('data', 'postgres', table, data, f'{table}.csv')
            df = pd.read_csv(filepath)
            df.to_sql(table, con = engine, if_exists = 'append', index = False)
        except Exception as ex:
            print(f'erro ao inserir {filepath} no database local : {ex}')

    try:
        filepath = os.path.join('data', 'csv', data, 'order_details.csv')
        df = pd.read_csv(filepath)
        df.to_sql('order_details', con = engine, if_exists = 'append', index = False)
    except Exception as ex:
        print(f'erro ao inserir {filepath} no database local: {ex}')

if __name__ == '__main__':
    from sys import argv

    if len(argv) < 6:
        print('use: ./transform.py <data> <database-local> <usuario> <senha> <porta>')
        exit(0)

    data = argv[1]
    database = argv[2]
    user = argv[3]
    password = argv[4]
    port = int(argv[5])
    
    transform(data, database, user, password, port)
    
