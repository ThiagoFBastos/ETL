#!/usr/bin/env python3

import pandas as pd
import os
import psycopg2
import psycopg2.extras

#dbconnect se conecta ao postgres local com o database northwind_output
def dbconnect(_database, _user, _password, _port):
    try:
        con = psycopg2.connect(host = 'localhost', port = _port, database = _database, user = _user, password = _password)
        return con
    except Exception as ex:
        print(f'erro ao se conectar com o database: {ex}')
        exit(0)

#dbexecute realiza um comando sql (select) e retorna as linhas correspondentes
def dbexecute(con, sql):
    cur = con.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
    cur.execute(sql)
    records = cur.fetchall()
    return records

"""
    executa um join nas tabelas orders e orders_details e armazenados
    o resultado num arquivo csv
"""
def load(data, database, user, password, port):
    with dbconnect(database, user, password, port) as con:
        try:
            directory = os.path.join('data', data, 'output')

            os.makedirs(directory, exist_ok = True)
            
            rows = dbexecute(con, 'select * from orders o inner join order_details od on o.order_id = od.order_id')

            df = pd.DataFrame(rows)

            df.to_csv(os.path.join(directory, 'order_details.csv'), index = False)
        except Exception as ex:
            print(f'erro ao realizar a query e gravar o resultado: {ex}')

if __name__ == '__main__':
    from sys import argv

    if len(argv) < 6:
        print('use: ./load.py <data> <database-local> <usuario> <senha> <porta>')
        exit(0)

    data = argv[1]
    database = argv[2]
    user = argv[3]
    password = argv[4]
    port = int(argv[5])

    load(data, database, user, password, port)
    
