ARG AIRFLOW_VERSION=2.10.4
ARG PYTHON_VERSION=3.11

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER airflow

COPY airflow/requirements.txt /tmp/requirements.txt

RUN python -m venv /opt/airflow/data_venv && \
    /opt/airflow/data_venv/bin/pip install --no-cache-dir -U pip setuptools wheel && \
    /opt/airflow/data_venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt
