CURRENT_DIR := $(shell pwd)

start_db:
	cd database && docker-compose up -d
	cd database && python3 create_table.py
	
start_server:
	prefect server start --expose

start_agent:
	prefect agent local start -p $(CURRENT_DIR) --show-flow-logs

create_project: 
	prefect create project "brt_watch"

deploy_flows:
	python3 pipeline/flows.py
