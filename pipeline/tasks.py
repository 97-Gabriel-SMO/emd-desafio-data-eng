import prefect
from prefect import task
import requests
import pandas as pd
import time
from sqlalchemy import create_engine
from tools import is_file_locked
import fcntl
    

@task
def make_api_request(api_url):
    logger = prefect.context.get("logger")
    response = requests.get(api_url)
    return response.json()


@task 
def storage_data(source_data: str, destination_path: str):
    logger = prefect.context.get("logger")
    
    try:
        with open(destination_path, 'r') as file:
            while is_file_locked(file):
                print(f"Arquivo {destination_path} em uso, aguardando liberação...")
                time.sleep(2)

            fcntl.flock(file.fileno(), fcntl.LOCK_EX)

            source_data = source_data.get("veiculos")
            source_df = pd.DataFrame(source_data)

            logger.info("Try to catch saved data")    
            destination_df = pd.read_csv(destination_path,index_col=0)
            result_df = pd.concat([destination_df,source_df],ignore_index=True)
            result_df.to_csv(destination_path)

            fcntl.flock(file.fileno(), fcntl.LOCK_UN)
            file.close()
            logger.info(f"Data saved in {destination_path}")
    except:
        logger.warning("No saved data")
        source_data = source_data.get("veiculos")
        result_df = pd.DataFrame(source_data)
        result_df.to_csv(destination_path)


@task 
def send_data_to_postgres(local_data: pd.DataFrame, connection_args: dict):

    user = connection_args["POSTGRES_USER"]
    password = connection_args["POSTGRES_PASSWORD"]
    host = connection_args["POSTGRES_HOST"]
    database = connection_args["POSTGRES_DB"]
    port = connection_args["POSTGRES_PORT"]

    connection_str = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(connection_str)

    local_data.rename(columns = {'dataHora':'datahora'}, inplace = True)

    table_name = 'brt_raw'
    local_data.to_sql(table_name, engine, if_exists='append', index=False)

    local_data.to_csv("postgresql_data.csv")

    pass

    
@task
def read_local_data(csv_filename):

    # Adquire uma trava exclusiva no arquivo .csv
    with open(csv_filename, 'r') as file:
        while is_file_locked(file):
            print(f"Arquivo {csv_filename} em uso, aguardando liberação...")
            time.sleep(2)
        fcntl.flock(file.fileno(), fcntl.LOCK_EX)

        df = pd.read_csv(csv_filename,index_col=0)
        new_df = pd.DataFrame(columns=df.columns)
        new_df.to_csv(csv_filename)

        # Libera a trava do arquivo
        fcntl.flock(file.fileno(), fcntl.LOCK_UN)

    return df
