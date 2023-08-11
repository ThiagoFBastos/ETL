#!/usr/bin/env python3

import psycopg2
import psycopg2.extras
import pandas as pd
import os

#dbconnect se conecta ao postgres remoto que tem o database northwind
def dbconnect():
    try:
        con = psycopg2.connect(host = 'localhost', port = 5432, database = 'northwind', user = 'northwind_user', password = 'thewindisblowing')
        return con
    except Exception as ex:
        print(f'erro ao se conectar com o database: {ex}')
        exit(0)

#dbexecute executa o comando sql (select) e retorna as linhas correspondentes
def dbexecute(con, sql):
    cur = con.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    cur.execute(sql)
    record = cur.fetchall()
    return record

"""
    extrai os dados das tabelas e do .csv 
    e os armazena localmente como arquivos csv
"""
def extract(data):

    tables = ['categories',
         'products',
         'suppliers',
         'employees',
         'employee_territories',
         'territories',
         'region',
         'orders',
         'shippers',
         'us_states',
         'customer_demographics',
         'customer_customer_demo',
         'customers'
     ]

    try:
        order_details_path = os.path.join('data', 'order_details.csv')

        df_csv = pd.read_csv(order_details_path)

        directory = os.path.join('data', 'csv', data)
        os.makedirs(directory, exist_ok = True)

        df_csv.to_csv(os.path.join(directory, 'order_details.csv'), index = False)
    except Exception as ex:
        print(f'erro ao extrair e gravar order_details.csv: {ex}')
        exit(0)

    with dbconnect() as con:
        for table in tables:
            try:
                directory = os.path.join('data', 'postgres', table, data)

                os.makedirs(directory, exist_ok = True)
             
                table_dict = dbexecute(con, f'select * from {table}')

                df_table = pd.DataFrame(table_dict)

                df_table.to_csv(os.path.join(directory, f'{table}.csv'), index = False)
            except Exception as ex:
                print(f'erro ao extrair e gravar a tabela {table}: {ex}')
                exit(0)

if __name__ == '__main__':
    from sys import argv

    if len(argv) < 2:
        print('use: ./extract.py <data>')
        exit(0)

    data = argv[1]

    extract(data)
