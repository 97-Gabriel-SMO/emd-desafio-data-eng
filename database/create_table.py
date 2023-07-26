import json
import psycopg2
import os

current_dir = os.getcwd()
config_dir = os.path.dirname(current_dir)
with open(config_dir+"/config.json", 'r') as arquivo:
    connection_args = json.load(arquivo)


def read_query_from_file(file_path):
    with open(file_path, 'r') as file:
        query = file.read()
    return query


# Função para criar a tabela
def create_table(query):
    try:
        # Conecta ao banco de dados
        connection = psycopg2.connect(
            host=connection_args["POSTGRES_HOST"],
            port=connection_args["POSTGRES_PORT"],
            database=connection_args["POSTGRES_DB"],
            user=connection_args["POSTGRES_USER"],
            password=connection_args["POSTGRES_PASSWORD"]
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
query = read_query_from_file(current_dir+"/query/create_brt_raw_data_table.sql")
create_table(query)