# FILE: Dockerfile
FROM apache/airflow:2.7.2-python3.10

COPY dags /opt/airflow/dags
COPY plugins /opt/airflow/plugins