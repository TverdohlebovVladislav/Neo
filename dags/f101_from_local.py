from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from core.constants import AIRFLOW_HOME
from core import FromLocal
from core import DEFAULT_POSTGRES_CONN_ID

with DAG(
    'f101_from_csv_to_db',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='download f101 data mart from database',
    template_searchpath=f"{AIRFLOW_HOME}/sql/",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 5),
    max_active_runs=1,
    catchup=False,
    tags=['fromDbToScv']
) as dag:

    send_f101_dm = FromLocal(
        table_name='dm_f101_round_f_v2',
        csv_path=f'{AIRFLOW_HOME}/csv/dm/dm_f101_round_f.csv',
        sql_to_update=f'{AIRFLOW_HOME}/sql/update_dm/f101_double_from_local.sql',
        date_corr_cols=['from_date', 'to_date'],
        schema='dm',
        replace_if_exists=True
    )

    start_task = DummyOperator(task_id='START', dag=dag)
    end_task = DummyOperator(task_id='END', dag=dag)

    update_db_f101_dm_task = PythonOperator(
        dag=dag,
        task_id="update_double_f101_dm_csv",
        python_callable=send_f101_dm.send_data
    )

    start_task >> \
    update_db_f101_dm_task >> \
    end_task
