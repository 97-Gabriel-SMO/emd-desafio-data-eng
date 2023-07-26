from prefect import Flow,task
from prefect.schedules import IntervalSchedule
from tasks import make_api_request,export_data
from datetime import timedelta


schedule = IntervalSchedule(interval=timedelta(minutes=1))


with Flow("exemplo",schedule=schedule) as flow:
    api_data = make_api_request("https://dados.mobilidade.rio/gps/brt")
    export_data(api_data,"pipeline/data/return.csv")

flow.register(project_name="teste")