.PHONY: install test lint init backfill report airflow-up airflow-down

install:
	python3 -m pip install -e '.[dev]'

test:
	python3 -m pytest

lint:
	python3 -m ruff check src tests airflow/dags

init:
	python3 -m airline_warehouse.cli init

backfill:
	python3 -m airline_warehouse.cli backfill --start "$${PIPELINE_START_MONTH:-2023-01}" --end "$${PIPELINE_END_MONTH:-2024-12}"

report:
	python3 -m airline_warehouse.cli report --output reports/pipeline_metrics.json --markdown reports/benchmark.md

airflow-up:
	docker compose -f airflow/docker-compose.yml up airflow-init
	docker compose -f airflow/docker-compose.yml up -d

airflow-down:
	docker compose -f airflow/docker-compose.yml down

