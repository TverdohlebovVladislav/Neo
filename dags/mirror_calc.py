from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import core

with DAG(
    'CSV_TO_DS',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Load csv files to detailed lay',
    schedule_interval='*/5 * * * *',
    start_date=datetime(2022, 5, 5),
    max_active_runs=1,
    template_searchpath=f"{core.AIRFLOW_HOME}/sql/",
    catchup=False,
    tags=['mirror'],
    dagrun_timeout=timedelta(seconds=5)
) as dag:

    mirror_md_account_d = core.Mirror(
        delta_dir=f"{core.AIRFLOW_HOME}/mirrors/md_account_d/data_deltas/",
        table_name='md_account_d',
        primary_key='ACCOUNT_RK'
    )


    start_task = DummyOperator(task_id='START', dag=dag)
    end_task = DummyOperator(task_id='END', dag=dag)
    load_mirror_md_account_d = PythonOperator(
        dag=dag,
        task_id=f"MAKE_MIRROR_md_account_d",
        python_callable=mirror_md_account_d.process_deltas,
    )


    start_task >> load_mirror_md_account_d >> end_task
