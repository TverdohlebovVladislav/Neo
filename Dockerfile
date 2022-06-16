FROM apache/airflow:2.2.3

USER root
RUN apt-get update -qq && apt-get install vim -qqq

ENV AIRFLOW_HOME=/opt/airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
USER airflow
