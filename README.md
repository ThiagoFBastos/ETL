# Requisitos

- python3
- biblioteca pandas
- biblioteca psycopg2
- postgres para usar como database local

# Uso

1. Primeiro deve-se importar o database northwind_output.sql para o postgres local
2. Depois rodar no terminal python3 main.py *argumentos ou ./main.py *argumentos
3. Os argumentos são: {data} {database-local} {usuario} {senha} {porta} sendo {data} a data para nomear as pastas, {database-local} o database local com todas as tabelas incluindo o order_details, {usuario} o usuário que detém o database, {senha} a senha do usuário e {porta} a porta pela qual o postgres está ouvindo
4. É possível rodar só o extrair (que armazena localmente em arquivo as tabelas e o csv) separadamente usando ./extract.py {data}
5. É possível rodar só o transformar para uma data , desde que o extrair já tenha sido realizado, separadamente: ./transform.py {data} {database-local} {usuario} {senha} {porta}
6. É possível rodar só o carregar para uma data , desde que o transformar já tenha sido realizado, separadamente: ./load.py {data} {database-local} {usuario} {senha} {porta}
7. A saída está na pasta data/{data informada}/output/order_details.csv com a junção do orders e order_details
