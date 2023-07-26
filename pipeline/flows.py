from prefect import Flow,task
from prefect.schedules import IntervalSchedule
from tasks import make_api_request,storage_data,read_local_data,send_data_to_postgres
from datetime import timedelta
import json
import os

local_storage_schedule = IntervalSchedule(interval=timedelta(minutes=1))
postgres_storage_schedule = IntervalSchedule(interval=timedelta(minutes=2))

current_dir = os.getcwd()

with open(current_dir+"/config.json", 'r') as arquivo:
    connection_args = json.load(arquivo)
   

with Flow("local_storage_flow",schedule=local_storage_schedule) as local_storage_flow:
    api_data = make_api_request("https://dados.mobilidade.rio/gps/brt")
    storage_data(api_data,"pipeline/data/return.csv")


with Flow("postgres_storage_flow",schedule=postgres_storage_schedule) as postgres_storage_flow:
    local_data = read_local_data("pipeline/data/return.csv")
    send_data_to_postgres(local_data,connection_args)



local_storage_flow.register(project_name="brt_watch")
postgres_storage_flow.register(project_name="brt_watch")
