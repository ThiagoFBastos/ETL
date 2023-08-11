#!/usr/bin/env python3

import extract as E
import transform as T
import load as L
from sys import argv

def main():
    if len(argv) < 6:
        print('use: ./main.py <data> <database-local> <usario> <senha> <porta>')
        exit(0)

    data = argv[1]
    database = argv[2]
    user = argv[3]
    password = argv[4]
    port = int(argv[5])

    E.extract(data) #extraí as tabelas e o csv e armazanando em arquivos

    T.transform(data, database, user, password, port) #insere os arquivos com as tabelas e o order_details no database local

    L.load(data, database, user, password, port) # realiza a query no database sobre a tabela orders e order_details

if __name__ == '__main__':
    main()
