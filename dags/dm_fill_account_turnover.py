from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from core import AIRFLOW_HOME, DEFAULT_POSTGRES_CONN_ID


with DAG(
    'DM-COUNT-account-turnover',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Count data mart "fill account turnover"',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 5),
    max_active_runs=1,
    template_searchpath=f"{AIRFLOW_HOME}/sql/",
    catchup=False,
    tags=['dm_count']
) as dag:

    start_task = DummyOperator(task_id='START', dag=dag)
    end_task = DummyOperator(task_id='END', dag=dag)
    go_to_count_dms = DummyOperator(task_id='COUNT_BY_EVERY_DAY_JAN', dag=dag)

    # ---
    # DM create necessary tables
    dm_start_settings = PostgresOperator(
        task_id="create_dm_schema_and_tables", 
        sql="create_tables_dm.sql", 
        postgres_conn_id=DEFAULT_POSTGRES_CONN_ID
    )

    # ---
    # Create necessary LOG tables
    log_start_settings = PostgresOperator(
        task_id="create_log_tables", 
        sql="create_table_logs.sql", 
        postgres_conn_id=DEFAULT_POSTGRES_CONN_ID
    )

    log_procedure_create = PostgresOperator(
        task_id="log_procedure_create", 
        sql="procedures/procedure_writelog.sql", 
        postgres_conn_id=DEFAULT_POSTGRES_CONN_ID
    )

    # ---
    # Create necessary procedure for count
    procedure_for_count_create = PostgresOperator(
        task_id="procedure_for_count_create", 
        sql="procedures/procedure_fill_account_turnover_f.sql", 
        postgres_conn_id=DEFAULT_POSTGRES_CONN_ID
    )


    # ---
    # Count every day by January 
    count_january = []
    for i in range(1, 31):
        count_january.append(
            PostgresOperator(
                task_id=f"START_COUNT_BY_DAY_{i}", 
                sql="CALL ds.fill_account_turnover_f (%(date)s::date);", 
                postgres_conn_id=DEFAULT_POSTGRES_CONN_ID,
                parameters={"date": f"2018-01-{i}"},
                autocommit=True
            )
        )


    # ---
    # Update columns if null value exists
    update_if_null = PostgresOperator(
        task_id=f"update_if_null", 
        sql="update_dm/turnover_f.sql", 
        postgres_conn_id=DEFAULT_POSTGRES_CONN_ID
    )


    start_task >> \
    [
        dm_start_settings,
        log_start_settings,
        procedure_for_count_create,
        log_procedure_create
    ] >> \
    go_to_count_dms >> \
    count_january >> \
    update_if_null >> \
    end_task