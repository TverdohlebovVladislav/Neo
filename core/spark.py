import sys
from os import getenv
import logging
from pyspark.sql import SparkSession
import os
from datetime import datetime
import pytz
from delta.tables import DeltaTable
from delta import *
import pyspark
import shutil


AIRFLOW_HOME = getenv('AIRFLOW_HOME', '/opt/airflow')

def get_min_max_delta_id(delta_dir):
    dirs_in_deltas = os.listdir(delta_dir)
    dirs_in_deltas_int = [int(dirs_in_deltas[i]) for i in range(len(dirs_in_deltas))]
    first_delt_id_dir = min(dirs_in_deltas_int)
    last_delt_id_dir = max(dirs_in_deltas_int)
    return first_delt_id_dir, last_delt_id_dir

class Mirror():

    def __init__(
        self, 
        delta_dir: str, 
        table_name: str, 
        primary_key: list, 
        source_from_postgres: str = None,
        source_from_local: str  = None
        ) -> None:
        # self.path_delts = path_delts

        self.table_name = table_name
        self.primary_key = primary_key
        self.delta_dir = delta_dir
        self.source_from_postgres = source_from_postgres
        self.source_from_local = source_from_local

        self.all_mirrors = f"{AIRFLOW_HOME}/mirrors"
        self.log_dir = f"{self.all_mirrors}/logs"
        self.final_version_path = f"{AIRFLOW_HOME}/mirrors/{self.table_name}/mirr_{self.table_name}/"
    
        # Создание spark сессии
        builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        # Создание иерархии папок зеркала
        if not os.path.isdir(f"{self.all_mirrors}/{self.table_name}"):
            os.umask(0)
            os.mkdir(f"{self.all_mirrors}/{self.table_name}")

            # ЗЕРКАЛО
            if not os.path.isdir(self.final_version_path):
                os.mkdir(self.final_version_path)
                if not (self.source_from_postgres and self.source_from_local):
                    min_max = get_min_max_delta_id(self.delta_dir)
                    # shutil.copy(self.delta_dir + f'{min_max[0]}/{self.table_name}.csv', self.final_version_path)
                    df = self.spark.read.option("delimiter", ";") \
                        .option("header", "true") \
                        .csv(self.delta_dir + f'{min_max[0]}/{self.table_name}.csv')
                    df.write.format("delta").mode("append").save(self.final_version_path)
                    
        # ЛОГИ
        if not os.path.isdir(self.log_dir):
            os.umask(0)
            os.mkdir(self.log_dir)
            min_max = get_min_max_delta_id(self.delta_dir)
            schema = ['delta_id', 'time_start', 'time_end', 'table_name']
            rows = [(min_max[0], 'null', 'null', self.table_name)]
            logs_table = self.spark.createDataFrame(rows, schema)
            logs_table.write.format("delta").mode("append").save(self.log_dir)

        # Создание дельт
        self.deltaTable = DeltaTable.forPath(self.spark, self.final_version_path)
        self.logTable = DeltaTable.forPath(self.spark, self.log_dir)


    def save_to_log(self, log_row: tuple) -> None:
        """
        Добавляет новую запись в таблицу логов 
        создания зеркал в mirrors/logs/{table_name}
        row_to_table - строка с новой записью вида (delta_id, start_time, end_time, table_name)
        """
        os.umask(0)
        schema = ['delta_id', 'time_start', 'time_end', 'table_name']
        logs_table = self.logTable.toDF()
        newRow = self.spark.createDataFrame([log_row], schema)

        # Генерация словаря для обнавления
        dict_to_update = dict()
        for field in newRow.schema.fields:
            if field.name != 'delta_id':
                dict_to_update[field.name] = f'upt.{field.name}'

        # Генерация словаря для вставки 
        dict_to_insert = dict()
        for field in newRow.schema.fields:
            dict_to_insert[field.name] = f'upt.{field.name}'

        self.logTable.alias("logs").merge(
            source = newRow.alias("upt"),
            condition = "logs.delta_id = upt.delta_id") \
                .whenMatchedUpdate(set=dict_to_update) \
                .whenNotMatchedInsert(values=dict_to_insert) \
                .execute()


    def process_deltas(self) -> None:
        """
        Обрабатывает все имеющиеся дельты в папке data_deltas 
        и сохраняет итоговый результат в mirr_md_acoount_d
        """
        tz_moscow = pytz.timezone("Europe/Moscow")
        
        #  1. Зайти в логи, посмотреть id полследней обработанной дельты 
        try:
            logs_table = self.logTable.toDF()
            last_delt_id_logs = logs_table.agg({"delta_id": "max"}).first()[0]
        except Exception as e:
            print(f"\n\n\n{e}\n\n\n")
            last_delt_id_logs = 0
        

        #  2. Посомтреть id посследней дельты в папке 
        try:
            dirs_in_deltas = os.listdir(self.delta_dir)
            dirs_in_deltas_int = [int(dirs_in_deltas[i]) for i in range(len(dirs_in_deltas))]
            first_delt_id_dir = min(dirs_in_deltas_int)

            last_delt_id_logs = last_delt_id_logs if last_delt_id_logs else first_delt_id_dir
            last_delt_id_dir = max(dirs_in_deltas_int)

        except Exception as e:
            print(f"{e}")
            last_delt_id_dir = 0


        # 3. Если появились новые дельты - обрабатываем их
        if  last_delt_id_logs < last_delt_id_dir:
            for i in range(last_delt_id_logs + 1, last_delt_id_dir + 1):
                time_start_load = datetime.now(tz_moscow)
                # Если нет зеркала, то создать на основе первой дельты 
                if last_delt_id_logs:
                    
                    updatesDF = self.spark.read.option("delimiter", ";") \
                        .option("header", "true") \
                        .csv(f'{self.delta_dir}/{i}/{self.table_name}_{str(i)[-1]}.csv')
                    
                    # Генерация словарей для обнавления и вставки
                    dict_to_update = dict()
                    dict_to_insert = dict()
                    for field in updatesDF.schema.fields:
                        if field.name not in self.primary_key:
                            dict_to_update[field.name] = f'deltas.{field.name}'
                        dict_to_insert[field.name] = f'deltas.{field.name}'

                    # Обработка дельты
                    self.deltaTable.alias("mirror").merge(
                        source = updatesDF.alias("deltas"),
                        condition = "mirror.ACCOUNT_RK = deltas.ACCOUNT_RK") \
                            .whenMatchedUpdate(set=dict_to_update) \
                            .whenNotMatchedInsert(values=dict_to_insert) \
                            .execute()

                # Запись в логи
                time_end_load = datetime.now(tz_moscow)
                log_row = (i, time_start_load, time_end_load, self.table_name)
                self.save_to_log(log_row)
                
    def create_delta(self):
        """
        Сравнивает зеркало и таблицу источник,
        если есть обновления создает новую дельту
        и сохраняет в data_deltas
        """
        pass

    def save_to_csv(self):

        os.umask(0)

        self.deltaTable.toDF() \
            .coalesce(1) \
            .write.format("csv")\
            .option("header", True) \
            .option("sep", ";") \
            .mode("overwrite") \
            .save(f"{self.all_mirrors}/{self.table_name}/csv/final")

        self.logTable.toDF() \
            .coalesce(1) \
            .write.format("csv")\
            .option("header", True) \
            .option("sep", ";") \
            .mode("overwrite") \
            .save(f"{self.all_mirrors}/{self.table_name}/csv/log")
        

mirr = Mirror(
    delta_dir=f"{AIRFLOW_HOME}/mirrors/data_deltas/",
    table_name='md_account_d',
    primary_key=['ACCOUNT_RK']
)

mirr.process_deltas()
mirr.save_to_csv()