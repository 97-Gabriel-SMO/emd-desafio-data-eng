import prefect
from prefect import task
import requests
import pandas as pd
from filelock import FileLock
import fcntl


@task
def make_api_request(api_url):
    logger = prefect.context.get("logger")
    response = requests.get(api_url)
    return response.json()


@task 
def export_data(source_data: str, destination_path: str):
    logger = prefect.context.get("logger")

    file_to_lock = open(destination_path, 'r+')
    fcntl.flock(file_to_lock.fileno(), fcntl.LOCK_EX)

    logger.info("Acquiring lock...")

    source_data = source_data.get("veiculos")
    source_df = pd.DataFrame(source_data)

    try:
        logger.info("Try to catch saved data")    
        destination_df = pd.read_csv(destination_path)
        result_df = pd.concat([destination_df,source_df],ignore_index=True)
        result_df.to_csv(destination_path)
    except:
        logger.warning("No saved data")
        result_df = source_df
        result_df.to_csv(destination_path)

    fcntl.flock(file_to_lock.fileno(), fcntl.LOCK_UN)
    file_to_lock.close()
    logger.info(f"Data saved in {destination_path}")

    
