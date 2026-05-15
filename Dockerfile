FROM apache/airflow:2.8.1-python3.11

USER airflow

# Install providers needed by DAG tasks
# dbt-postgres is heavy — pinned to skip dependency resolution
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres==5.10.0 \
    apache-airflow-providers-slack==8.5.0 \
    psycopg2-binary==2.9.9 \
    pandas==2.1.4 \
    "dbt-postgres==1.7.14"
