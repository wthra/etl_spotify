#!/bin/bash
set -e

airflow db migrate

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@spotify-etl.local \
    --password admin \
    2>/dev/null || true

airflow variables set spotify_db_conn_id postgres_default 2>/dev/null || true
airflow variables set spotify_date_format "%Y-%m-%d" 2>/dev/null || true

airflow connections add postgres_default \
    --conn-type postgres \
    --conn-host postgres \
    --conn-login airflow \
    --conn-password airflow_password \
    --conn-port 5432 \
    --conn-schema airflow \
    2>/dev/null || true

exec "$@"
