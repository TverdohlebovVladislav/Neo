import sys

print(sys.executable)


from core import Mirror, AIRFLOW_HOME

mirr = Mirror(
    delta_dir=f"{AIRFLOW_HOME}/mirrors/md_account_d/data_deltas/",
    table_name='md_account_d',
    primary_key='ACCOUNT_RK'
)

mirr.process_deltas()