import pandas as pd
from sqlalchemy.exc import DataError

# Data to connerct to db
LOGIN = 'airflow'
PASSWORD = 'airflow'
HOST = '127.0.0.1'
PORT = '5432'
SCHEMA = 'postgres'


def download_csv(sql: str) -> None:
    jdbc_url = f"postgresql://{LOGIN}:{PASSWORD}@{HOST}:{PORT}/{SCHEMA}"
    try:
        df = pd.read_sql(
            sql=sql,
            con=jdbc_url
        )
        df.to_csv('result.csv', sep=';')
    except DataError:
        print('\tThe date format isn\'t correct! Try again!')
        

def main():
    repeat = True
    while (repeat):
        print(
            """
            ------
            Script counts MAX and MIN value 
            of credit_amount and debet_amount 
            for the date you specified.
            ------

            """
        )
        date = input('\tInput date in format like YYYY-MM-DD: ')
        download_csv(f'SELECT * from get_max_min_credit_debit_from_date(\'{date}\'::date);')
        repeat = bool(input('Do you want to continiue? (1 - Yes, 0 - No): '))

main()