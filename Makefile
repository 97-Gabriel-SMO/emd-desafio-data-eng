CURRENT_DIR := $(shell pwd)

start_db:
	docker-compose up -d
	sleep 5
	cd database && python3 create_table.py
	
start_server:
	prefect server start --expose -d

start_agent:
	prefect agent local start -p $(CURRENT_DIR)/pipeline --show-flow-logs

create_project: 
	prefect create project "brt_watch"

deploy_flows:
	python3 pipeline/flows.py

run_dbt:
	cd dbt/brt_watch && dbt run