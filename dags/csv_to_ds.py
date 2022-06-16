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
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 5),
    max_active_runs=3,
    template_searchpath=f"{core.AIRFLOW_HOME}/sql/",
    catchup=False,
    tags=['csv_to_ds']
) as dag:

    ft_balance_f = core.CsvToDs(
        "ft_balance_f",
        f"{core.AIRFLOW_HOME}/csv/ft_balance_f.csv",
        f'{core.AIRFLOW_HOME}/sql/update_ds/ft_balance_f.sql',
        ['on_date'],
    )

    ft_posting_f = core.CsvToDs(
        "ft_posting_f",
        f"{core.AIRFLOW_HOME}/csv/ft_posting_f.csv",
        f'{core.AIRFLOW_HOME}/sql/update_ds/ft_posting_f.sql',
        ['oper_date'],
        True
    )

    md_account_d = core.CsvToDs(
        "md_account_d",
        f"{core.AIRFLOW_HOME}/csv/md_account_d.csv",
        f'{core.AIRFLOW_HOME}/sql/update_ds/md_account_d.sql',
        ['data_actual_date', 'data_actual_end_date']
    )

    md_currency_d = core.CsvToDs(
        "md_currency_d",
        f"{core.AIRFLOW_HOME}/csv/md_currency_d.csv",
        f'{core.AIRFLOW_HOME}/sql/update_ds/md_currency_d.sql',
        ['data_actual_date', 'data_actual_end_date']
    )

    md_exchange_rate_d = core.CsvToDs(
        "md_exchange_rate_d",
        f"{core.AIRFLOW_HOME}/csv/md_exchange_rate_d.csv",
        f'{core.AIRFLOW_HOME}/sql/update_ds/md_exchange_rate_d.sql',
        ['data_actual_date', 'data_actual_end_date'],
        True
    )

    md_ledger_account_s = core.CsvToDs(
        "md_ledger_account_s",
        f"{core.AIRFLOW_HOME}/csv/md_ledger_account_s.csv",
        f'{core.AIRFLOW_HOME}/sql/update_ds/md_ledger_account_s.sql',
        ['start_date', 'end_date']
    )


    start_task = DummyOperator(task_id='START', dag=dag)
    end_task = DummyOperator(task_id='END', dag=dag)
    go_to_load_data = DummyOperator(task_id='Go_to_load_data', dag=dag)

    
    # ---
    # Create schema and tables for DS
    t1 = PostgresOperator(
        task_id="create_schema_and_tables_ds", 
        sql="create_tables_ds.sql", 
        postgres_conn_id=core.DEFAULT_POSTGRES_CONN_ID
    )

    # ---
    # Create schema and table for LOGS
    t2 = PostgresOperator(
        task_id="create_schema_and_tables_logs", 
        sql="create_table_logs.sql", 
        postgres_conn_id=core.DEFAULT_POSTGRES_CONN_ID
    )

    # ---
    # Send ft_balance_f
    load_ft_balance_f = PythonOperator(
        dag=dag,
        task_id=f"SEND_DS_{ft_balance_f.table_name}",
        python_callable=ft_balance_f.send_data,
    )

    # ---
    # Send ft_posting_f
    load_ft_posting_f = PythonOperator(
        dag=dag,
        task_id=f"SEND_DS_{ft_posting_f.table_name}",
        python_callable=ft_posting_f.send_data,
    )

    # ---
    # Send md_account_d
    load_md_account_d = PythonOperator(
        dag=dag,
        task_id=f"SEND_DS_{md_account_d.table_name}",
        python_callable=md_account_d.send_data
    )

    # ---
    # Send md_currency_d
    load_md_currency_d = PythonOperator(
        dag=dag,
        task_id=f"SEND_DS_{md_currency_d.table_name}",
        python_callable=md_currency_d.send_data,
    )

    # ---
    # Send md_exchange_rate_d
    load_md_exchange_rate_d = PythonOperator(
        dag=dag,
        task_id=f"SEND_DS_{md_exchange_rate_d.table_name}",
        python_callable=md_exchange_rate_d.send_data,
    )

    # ---
    # Send md_ledger_account_s
    load_md_ledger_account_s = PythonOperator(
        dag=dag,
        task_id=f"SEND_DS_{md_ledger_account_s.table_name}",
        python_callable=md_ledger_account_s.send_data,
    )


    start_task >> \
    [t1, t2] >> \
    go_to_load_data >> \
    [
        load_ft_balance_f,
        load_ft_posting_f,
        load_md_account_d,
        load_md_currency_d,
        load_md_exchange_rate_d,
        load_md_ledger_account_s,
    ] >> \
    end_task