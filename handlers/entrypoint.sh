#!/bin/bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command python) pip install --upgrade pip
  $(command -v pip) install --user -r requirements.txt
fi

if [ ! -f "/opt/airflow/airflow.db" ]; then
  airflow db init && \
  airflow users create \
    --username admin_new \
    --firstname admin_1 \
    --lastname admin_1 \
    --role Admin \
    --email admin_new@example.com \
    --password admin_1
fi

$(command -v airflow) db upgrade

exec airflow webserver