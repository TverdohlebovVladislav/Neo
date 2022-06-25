import time
import logging
import pytz
import pandas as pd
from sqlalchemy import *
from datetime import datetime
from core import LocalDbConnection


class FromLocal(LocalDbConnection):
    """
    Class to send csv data from local to 
    DS schema in database
    """

    def __init__(
        self, 
        table_name: str, 
        csv_path: str, 
        sql_to_update: str, 
        schema: str = 'temp',
        date_corr_cols: list = None, 
        save_index: bool = False,
        replace_if_exists: bool = False
        ) -> None:
        """
        table_name - name of table in DB
        csv_path - path to csv which will upload
        date_corr_cols - the columns with date in str type in csv
        sql_to_update_ds - path to sql file, wich update ds schema from temp schema
        save_index - determines whether the index from csv will be saved or not
        replace_if_exists - replace data if it esists in a table,
        schema - schema which data will upload if replace_if_exists set on True
        """
        super().__init__(
            table_name = table_name,
            csv_path = csv_path
        )

        self.save_index = save_index
        self.sql_to_update_ds = sql_to_update
        self.date_corr_cols = date_corr_cols
        self.replace_if_exists = replace_if_exists
        self.schema = schema


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
            if self.replace_if_exists:
                df.to_sql(self.table_name, self.engine, schema=self.schema, if_exists="replace", index=False)
            else:
                df.to_sql(self.table_name, self.engine, schema="temp", if_exists="replace", index=False)
                with open(self.sql_to_update_ds) as file:
                    sql = file.read()
                conn.execute(sql)
        except Exception as e:
            logging.info(e)
            self.condition += str(e)
        else:
            inf = f" Table {self.table_name} in has updated success. "
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
        
