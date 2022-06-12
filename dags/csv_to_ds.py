from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
import pandas as pd
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from os import getenv
import time
import logging
import pytz
from sqlalchemy import exc
from sqlalchemy import insert

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy.dialects.mysql import insert

DEFAULT_POSTGRES_CONN_ID = "postgres"
AIRFLOW_HOME = getenv('AIRFLOW_HOME', '/opt/airflow')

"""
1. Написать скрипт создания таблиц
2. Написать функцию, которая будет отправлять данные на сервер 
3. Написать SQL созадния таблицы логов
4. Написать функцию логирования и встроить ее в функцию добавления в базу данных
"""

"""
Переместить в файл в CORE - Logs_SQLAlchemy_connections.py
"""
from sqlalchemy import *
from sqlalchemy.orm import create_session
from sqlalchemy.schema import Table, MetaData
from sqlalchemy.ext.declarative import declarative_base



# #Create a session to use the tables    
# session = create_session(bind=engine)


"""
Переместить в файл в CORE - Send_csv_to_ds.py
"""
def insert_on_duplicate(table, conn, keys, data_iter):
    insert_stmt = insert(table.table).values(list(data_iter))
    # on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(insert_stmt.inserted)
    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(dict(insert_stmt.inserted))
    conn.execute(on_duplicate_key_stmt)


def send_csv_to_ds(table_name: str, csv_path: str, date_to_correct_format_columns: list) -> None:
    """
    Sends csv file to detailed layer in DB.
    Example of date_to_correct_format_columns: ['column_1', 'column_2', ...]
    """
    #Create and engine and get the metadata
    Base = declarative_base()
    conn_object = BaseHook.get_connection(DEFAULT_POSTGRES_CONN_ID)
    jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
                f"{conn_object.host}:{conn_object.port}/{conn_object.schema}"
    engine = create_engine(jdbc_url)
    metadata = MetaData(bind=engine, schema="logs")

    #Reflect each database table we need to use, using metadata
    class LogsLoadCsvToDs(Base):
        __table__ = Table('load_csv_to_ds', metadata, autoload=engine, schema='logs')

    tz_moscow = pytz.timezone("Europe/Moscow")

    # Connect to db
    conn_object = BaseHook.get_connection(DEFAULT_POSTGRES_CONN_ID)
    jdbc_url = f"postgresql://{conn_object.login}:{conn_object.password}@" \
               f"{conn_object.host}:{conn_object.port}/{conn_object.schema}"
    engine = create_engine(jdbc_url)

    # Session to load logs
    session = create_session(bind=engine)
    time_start_load = datetime.now(tz_moscow)
    condition = ''

    # Read  data
    try:
        # Read and drop default indexes
        df = pd.read_csv(csv_path, delimiter=";", index_col=False)
        df = df.drop([df.columns[0]], axis='columns')
        df.columns = map(str.lower, df.columns)

        # Correct date type in columns
        for i in range(len(date_to_correct_format_columns)):
            df[date_to_correct_format_columns[i].lower()] = pd.to_datetime(df[date_to_correct_format_columns[i].lower()])
            logging.info(f"Column {date_to_correct_format_columns[i]} changed date-time format")
            
    except Exception as e:
        logging.info(e)
        condition += str(e)
    else:
        inf = f"Table {table_name} read in pandas success."
        logging.info(inf)
        logging.info(f"\n {df.head()}")
        condition += inf

    # Send data to db schema
    try:
        df.to_sql(table_name, engine, schema="ds", if_exists="append", index=False, method=insert_on_duplicate)
    except Exception as e:
        logging.info(e)
        condition += str(e)
    else:
        inf = f"Table {table_name} in ds schema has updated success."
        logging.info(inf)
        condition += inf
    time.sleep(5)

    # Load to log table in DB
    time_end_load = datetime.now(tz_moscow)
    ins = insert(LogsLoadCsvToDs).values(
        # id = 1,
        table_name = table_name,
        csv_path = csv_path,
        time_start_load = time_start_load,
        time_end_load = time_end_load,
        condition = condition
    )
    conn = engine.connect()
    conn.execute(ins)
    session.close()



with DAG(
    'CSV_TO_DS',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Load csv files to detailed layer',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 5),
    max_active_runs=3,
    template_searchpath=f"{AIRFLOW_HOME}/sql/",
    catchup=False,
    tags=['csv_to_ds'],
) as dag:

    start_task = DummyOperator(task_id='START', dag=dag)
    end_task = DummyOperator(task_id='END', dag=dag)
    go_to_load_data = DummyOperator(task_id='Go_to_load_data', dag=dag)

    ft_balance_f = "ft_balance_f"

    # ---
    # Create schema and tables for DS
    t1 = PostgresOperator(
        task_id="create_schema_and_tables_ds", 
        sql="create_tables_ds.sql", 
        postgres_conn_id=DEFAULT_POSTGRES_CONN_ID
    )

    # ---
    # Create schema and table for LOGS
    t2 = PostgresOperator(
        task_id="create_schema_and_tables_logs", 
        sql="create_table_logs.sql", 
        postgres_conn_id=DEFAULT_POSTGRES_CONN_ID
    )

    # ---
    # Load tables to DS
    load_table_ft_balance_f = PythonOperator(
        dag=dag,
        task_id=f"SEND_DS_{ft_balance_f}",
        python_callable=send_csv_to_ds,
        op_kwargs={
            "table_name": f"{ft_balance_f}",
            "csv_path": f"{AIRFLOW_HOME}/csv/{ft_balance_f}.csv",
            "date_to_correct_format_columns": ['on_date']
        }
    )

    
    
    


    start_task >> [t1, t2] >> go_to_load_data >> load_table_ft_balance_f >> end_task