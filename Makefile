.PHONY: airflow-init airflow-up airflow-trigger airflow-down

airflow-init:
	docker compose -f docker-compose.airflow.yml up airflow-init

airflow-up:
	docker compose -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler

airflow-trigger:
	docker compose -f docker-compose.airflow.yml exec airflow-scheduler airflow dags trigger weather_de_pipeline

airflow-down:
	docker compose -f docker-compose.airflow.yml down
