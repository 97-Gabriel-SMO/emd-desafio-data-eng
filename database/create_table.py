import json
import psycopg2
import os

current_dir = os.getcwd()
config_dir = os.path.dirname(current_dir)
with open(config_dir+"/config.json", 'r') as arquivo:
    connection_args = json.load(arquivo)


def read_query_from_file(file_path):
    """ 
    This function retrieves a query from the path received as input
    """
    with open(file_path, 'r') as file:
        query = file.read()
    return query


def create_table(query):
    """
    This function executes the query, creating the table in the database
    """
    try:
        connection = psycopg2.connect(
            host=connection_args["POSTGRES_HOST"],
            port=connection_args["POSTGRES_PORT"],
            database=connection_args["POSTGRES_DB"],
            user=connection_args["POSTGRES_USER"],
            password=connection_args["POSTGRES_PASSWORD"]
        )
        
        cursor = connection.cursor()
        cursor.execute(query)
        connection.commit()
        
        print("Tabela criada com sucesso!")
        
    except (Exception, psycopg2.Error) as error:
        print("Erro ao criar a tabela:", error)
        
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

query = read_query_from_file(current_dir+"/query/create_brt_raw_data_table.sql")
create_table(query)