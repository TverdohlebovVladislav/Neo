import logging
import pandas as pd
from sqlalchemy import *
from datetime import datetime
from core import LocalDbConnection


class ToLocal(LocalDbConnection):
    """
    Class to download data from db to local
    DS schema in database
    """

    def __init__(self, table_name: str, csv_path: str, schema: str = 'dm') -> None:
        """
        table_name - name of table in DB
        csv_path - path to save csv 
        schema - schema of table to download
        """
        super().__init__(
            table_name=table_name,
            csv_path=csv_path
        )
        self.schema = schema

    def save_to_local(self):
        """
        Save data from DB to сым_зфер
        and write logs in DB
        """
        conn = self.engine.connect()
        time_start_load = datetime.now()

        try:
            df = pd.read_sql_table(
                self.table_name,
                self.engine,
                schema = self.schema
            )
            df.to_csv(self.csv_path, sep=';')
        except Exception as e:
            logging.info(e)
            self.condition += str(e)
        else:
            messege = f'Table {self.table_name} has alredy downloaded in {self.csv_path}. '
            logging.info(messege)
            self.condition += messege
        
        # Load to log table in DB
        time_end_load = datetime.now()
        ins = insert(self.LogsLoadCsvToDs).values(
            table_name = self.table_name,
            csv_path = self.csv_path,
            time_start_load = time_start_load,
            time_end_load = time_end_load,
            condition = self.condition
        )
        conn.execute(ins)
        pass
