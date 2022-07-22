from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
# from airflow.providers.postgres.operators.spark import SparkSubmitOperator

from os import getenv
AIRFLOW_HOME = getenv('AIRFLOW_HOME', '/opt/airflow')

_config = {
    'packages': 'io.delta:delta-core_2.12:1.0.0',
    'application': '/opt/airflow/core/test.py',
    'application_args': [
        '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',
        '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension'
    ]
}


with DAG(
    'MIRR_MD_ACCOUNT_D',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval='*/30 * * * *',
    start_date=datetime(2022, 5, 5),
    # max_active_runs=1,
    catchup=False,
    tags=['mirror'],
    # dagrun_timeout=timedelta(seconds=5)
) as dag:

    # spark-submit --master spark://spark:7077 /usr/local/spark/core/mirr_md_account_d.py

    start_task = DummyOperator(task_id='START', dag=dag)
    end_task = DummyOperator(task_id='END', dag=dag)

    load_mirror_md_account_d = BashOperator(
        dag=dag,
        task_id='load_mirror_md_account_d',
        bash_command='spark-submit --packages io.delta:delta-core_2.12:1.0.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" /opt/airflow/core/test.py'
        # bash_command='"${SPARK_HOME}/bin/spark-submit" --master spark://spark:7077 ' + AIRFLOW_HOME +'/spark_scripts/mirr_md_account_d.py'
    )

    # load_mirror_md_account_d = SparkSubmitOperator(
    #     task_id='spark_submit_job',
    #     dag=dag,
    #     **_config
    # )

    # docker exec -it neo_project_spark_1 spark-submit --packages io.delta:delta-core_2.12:1.0.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"   --master spark://spark:7077 /usr/local/spark/core/mirr_md_account_d.py
    start_task >> load_mirror_md_account_d >> end_task
