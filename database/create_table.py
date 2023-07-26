import os
import psycopg2

user = 'myuser'
password = 'mypassword'
host = 'localhost'
database = 'mydatabase'
port = '5400'  # Porta padrão do PostgreSQL


def read_query_from_file(file_path):
    with open(file_path, 'r') as file:
        query = file.read()
    return query


# Função para criar a tabela
def create_table(query):
    try:
        # Conecta ao banco de dados
        connection = psycopg2.connect(
            host=host, port=port, database=database, user=user, password=password
        )
        
        # Cria um cursor para executar comandos SQL
        cursor = connection.cursor()
        
        # Executa o comando para criar a tabela
        cursor.execute(query)
        
        # Confirma a transação
        connection.commit()
        
        print("Tabela criada com sucesso!")
        
    except (Exception, psycopg2.Error) as error:
        print("Erro ao criar a tabela:", error)
        
    finally:
        # Fecha o cursor e a conexão
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Chama a função para criar a tabela
query = read_query_from_file("query/create_brt_raw_data_table.sql")
create_table(query)