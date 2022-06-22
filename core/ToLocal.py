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
from core import LocalDbConnection


class ToLocal(LocalDbConnection):
    """
    Class to send csv data from local to 
    DS schema in database
    """

    def __init__(self, table_name: str, csv_path: str, schema: str = None) -> None:
        """
        table_name - name of table in DB
        csv_path - path to csv which will upload
        """
        super.__init__(
            table_name = table_name,
            csv_path = csv_path
        )

        self.schema = schema

        # self.save_index = save_index
        # self.sql_to_update_ds = sql_to_update
        # self.date_corr_cols = date_corr_cols

    def save_to_local(self):
        
        pass


        
