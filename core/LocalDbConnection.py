from sqlalchemy import create_engine
from sqlalchemy import *
from sqlalchemy.schema import Table, MetaData
from sqlalchemy.ext.declarative import declarative_base
from core import CONN_OBJECT



class LocalDbConnection():
    """
    A base class for organizing the 
    interaction of local storage and database
    """

    def __init__(self, table_name: str, csv_path: str) -> None:
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
        self.condition = ''
        self.engine = self.get_engine()
        self.LogsLoadCsvToDs = self.log_table_upload_conn()
    
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
        
