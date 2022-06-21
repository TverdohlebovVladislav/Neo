from sqlalchemy import create_engine
import time
import logging
import pytz
import pandas as pd
from sqlalchemy import *
from sqlalchemy.orm import create_session
from sqlalchemy.schema import Table, MetaData
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base
from core import CONN_OBJECT


class CsvToDs():
    """
    Class to send csv data from local to 
    DS schema in database
    """

    def __init__(self, table_name: str, csv_path: str, sql_to_update: str, date_corr_cols: list = None, save_index = False) -> None:
        """
        table_name - name of table in DB
        csv_path - path to csv which will upload
        date_corr_cols - the columns with date in str type in csv
        sql_to_update_ds - path to sql file, wich update ds schema from temp schema
        condition - text of log in log table in DB
        engine - engine variable to connect with sqlalchemy to DB
        LogsLoadCsvToDs - class for upload data to logs table in logs schema
        save_index - determines whether the index from csv will be saved or not
        """
        self.table_name = table_name
        self.csv_path = csv_path
        self.date_corr_cols = date_corr_cols
        self.sql_to_update_ds = sql_to_update
        self.condition = ''
        self.engine = self.get_engine()
        self.LogsLoadCsvToDs = self.log_table_upload_conn()
        self.save_index = save_index

    def get_df(self) -> pd.DataFrame:
        """
        Get csv to pandas dataframe
        """
        try:
            # Read and drop default indexes
            df = pd.read_csv(self.csv_path, delimiter=";", index_col=False)
            if self.save_index:
                df.rename(columns={df.columns[0]: 'id'}, inplace=True)
                logging.info(df.head())
            else:
                df = df.drop([df.columns[0]], axis='columns')
            df.columns = map(str.lower, df.columns)

            # Correct date type in columns
            if self.date_corr_cols:
                for i in range(len(self.date_corr_cols)):
                    df[self.date_corr_cols[i].lower()] = pd.to_datetime(df[self.date_corr_cols[i].lower()])
                    logging.info(f"Column {self.date_corr_cols[i]} changed date-time format")
        except Exception as e:
            logging.info(e)
            self.condition += str(e)
        else:
            inf = f"Table {self.table_name} read in pandas success. "
            logging.info(inf)
            logging.info(f"\n {df.head()}")
            self.condition += inf
            return df
    
    def get_engine(self):
        """
        Create engine to connect with DB
        """
        jdbc_url = f"postgresql://{CONN_OBJECT.login}:{CONN_OBJECT.password}@" \
                f"{CONN_OBJECT.host}:{CONN_OBJECT.port}/{CONN_OBJECT.schema}"
        return create_engine(jdbc_url)

    def log_table_upload_conn(self):
        """
        Declare and return object of class for logs table to upload in logs schema
        """
        Base = declarative_base()
        metadata = MetaData(bind=self.get_engine(), schema="logs")
        if self.get_engine().dialect.has_table(self.get_engine(), 'load_csv_to_ds', schema='logs'):
            class LogsLoadCsvToDs(Base):
                __table__ = Table('load_csv_to_ds', metadata, autoload=self.get_engine(), schema='logs')
            return LogsLoadCsvToDs

    def send_data(self):
        """
        Send pandas dataframe to temp schema and 
        then send to ds schema with write logs
        """
        tz_moscow = pytz.timezone("Europe/Moscow")
        conn = self.engine.connect()
        time_start_load = datetime.now(tz_moscow)

        df = self.get_df()
        try:
            df.to_sql(self.table_name, self.engine, schema="temp", if_exists="replace", index=False)
            with open(self.sql_to_update_ds) as file:
                sql = file.read()
            conn.execute(sql)
        except Exception as e:
            logging.info(e)
            self.condition += str(e)
        else:
            inf = f" Table {self.table_name} in ds schema has updated success. "
            logging.info(inf)
            self.condition += inf
        time.sleep(5)

        # Load to log table in DB
        time_end_load = datetime.now(tz_moscow)
        ins = insert(self.LogsLoadCsvToDs).values(
            table_name = self.table_name,
            csv_path = self.csv_path,
            time_start_load = time_start_load,
            time_end_load = time_end_load,
            condition = self.condition
        )
        
        conn.execute(ins)
        
