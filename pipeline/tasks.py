import prefect
from prefect import task
import requests
import pandas as pd
import time
from sqlalchemy import create_engine
from tools import is_file_locked
import fcntl
    

@task
def make_api_request(api_url: str, max_retries=3, retry_delay=1):
    """
    Performs the request to the API and returns the received value
    """

    logger = prefect.context.get("logger")
    attempts = 0
    while attempts < max_retries:
        try:
            response = requests.get(api_url)
            response.raise_for_status()  
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error: {e}")
            attempts += 1
            if attempts < max_retries:
                logger.info(f"Trying again in {retry_delay} seconds...")
                time.sleep(retry_delay)
    
    logger.error("Maximum number of attempts reached. The request failed.")
    return None


@task 
def storage_data(source_data: str, destination_path: str):
    """
    Performs local data storage
    """

    logger = prefect.context.get("logger")
    try:
        with open(destination_path, 'r') as file:
            while is_file_locked(file):
                logger.info(f"File {destination_path} in use, waiting for release...")
                time.sleep(5)

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
        logger.info("Creating new .csv ...")
        source_data = source_data.get("veiculos")
        result_df = pd.DataFrame(source_data)
        result_df.to_csv(destination_path)


@task 
def send_data_to_postgres(local_data: pd.DataFrame, connection_args: dict):
    """
    Send the local data to Postgres
    """
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

    pass

    
@task
def read_local_data(csv_filename: str):
    """
    Performs local data reading
    """
    logger = prefect.context.get("logger")
    with open(csv_filename, 'r') as file:
        while is_file_locked(file):
            logger.info(f"File {csv_filename} in use, waiting for release...")
            time.sleep(5)
        fcntl.flock(file.fileno(), fcntl.LOCK_EX)
        df = pd.read_csv(csv_filename,index_col=0)
        new_df = pd.DataFrame(columns=df.columns)
        new_df.to_csv(csv_filename)
        fcntl.flock(file.fileno(), fcntl.LOCK_UN)
    logger = prefect.context.get("Local data has been read successfully")
    return df
