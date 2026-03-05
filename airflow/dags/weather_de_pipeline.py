from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/weather-de"
DATA_VENV_BIN = "/opt/airflow/data_venv/bin"

runtime_env = {
    "POSTGRES_HOST": os.getenv("POSTGRES_HOST", "postgres"),
    "POSTGRES_PORT": os.getenv("POSTGRES_PORT", "5432"),
    "POSTGRES_DB": os.getenv("POSTGRES_DB", "airflow"),
    "POSTGRES_USER": os.getenv("POSTGRES_USER", "airflow"),
    "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "airflow"),
    "DBT_PROFILES_DIR": f"{PROJECT_DIR}/airflow/dbt",
}

with DAG(
    dag_id="weather_de_pipeline",
    description="Open-Meteo historical data pipeline: ingest -> bronze -> dbt marts",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["weather", "dbt", "data-engineering"],
    params={
        "weather_start_date": None,
        "weather_end_date": None,
        "weather_lookback_days": 90,
    },
) as dag:
    fetch_open_meteo_data = BashOperator(
        task_id="fetch_open_meteo_data",
        bash_command=f"""
cd {PROJECT_DIR}
set -euo pipefail

START_DATE="{{{{ dag_run.conf.get('weather_start_date', params.weather_start_date) if dag_run else params.weather_start_date }}}}"
END_DATE="{{{{ dag_run.conf.get('weather_end_date', params.weather_end_date) if dag_run else params.weather_end_date }}}}"
LOOKBACK_DAYS="{{{{ dag_run.conf.get('weather_lookback_days', params.weather_lookback_days) if dag_run else params.weather_lookback_days }}}}"

if [[ -n "$START_DATE" && -n "$END_DATE" ]]; then
  WEATHER_START_DATE="$START_DATE" WEATHER_END_DATE="$END_DATE" {DATA_VENV_BIN}/python ingest/weather_fetch.py
elif [[ -n "$START_DATE" || -n "$END_DATE" ]]; then
  echo "Both weather_start_date and weather_end_date are required when specifying a fixed date range."
  exit 1
else
  WEATHER_LOOKBACK_DAYS="$LOOKBACK_DAYS" {DATA_VENV_BIN}/python ingest/weather_fetch.py
fi
""",
        env=runtime_env,
    )

    convert_jsonl_to_parquet = BashOperator(
        task_id="convert_jsonl_to_parquet",
        bash_command=f"cd {PROJECT_DIR} && {DATA_VENV_BIN}/python ingest/convert_jsonl_to_parquet.py",
        env=runtime_env,
    )

    load_bronze_to_postgres = BashOperator(
        task_id="load_bronze_to_postgres",
        bash_command=f"cd {PROJECT_DIR} && {DATA_VENV_BIN}/python ingest/postgres_loader.py --schema bronze --mode upsert",
        env=runtime_env,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {PROJECT_DIR} && {DATA_VENV_BIN}/dbt deps",
        env=runtime_env,
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {PROJECT_DIR} && {DATA_VENV_BIN}/dbt source freshness",
        env=runtime_env,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {PROJECT_DIR} && {DATA_VENV_BIN}/dbt run",
        env=runtime_env,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {PROJECT_DIR} && {DATA_VENV_BIN}/dbt test",
        env=runtime_env,
    )

    (
        fetch_open_meteo_data
        >> convert_jsonl_to_parquet
        >> load_bronze_to_postgres
        >> dbt_deps
        >> dbt_source_freshness
        >> dbt_run
        >> dbt_test
    )
