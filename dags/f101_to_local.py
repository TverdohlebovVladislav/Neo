from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from core.constants import AIRFLOW_HOME
from core import ToLocal


with DAG(
    'f101_to_csv',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='download f101 data mart from database',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 5),
    max_active_runs=1,
    catchup=False,
    tags=['fromDbToScv']
) as dag:

    download_f101_dm = ToLocal(
        table_name='dm_f101_round_f',
        csv_path=f'{AIRFLOW_HOME}/csv/dm/dm_f101_round_f.csv',
        schema='dm'
    )

    start_task = DummyOperator(task_id='START', dag=dag)
    end_task = DummyOperator(task_id='END', dag=dag)

    download_f101_dm_task = PythonOperator(
        dag=dag,
        task_id="download_f101_dm_csv",
        python_callable=download_f101_dm.save_to_local,
    )

    start_task >> \
    download_f101_dm_task >> \
    end_task
