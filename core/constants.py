from os import getenv
from airflow.hooks.base import BaseHook

DEFAULT_POSTGRES_CONN_ID = "postgres"
AIRFLOW_HOME = getenv('AIRFLOW_HOME', '/opt/airflow')
CONN_OBJECT = BaseHook.get_connection(DEFAULT_POSTGRES_CONN_ID)
