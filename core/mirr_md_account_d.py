import sys
from os import getenv

AIRFLOW_HOME = getenv('AIRFLOW_HOME', '/opt/airflow')
print(sys.executable)


from pyspark.sql import SparkSession
import os
# from constants import AIRFLOW_HOME
from datetime import datetime
import pytz
from delta.tables import DeltaTable
from delta import *
import pyspark


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
        self.delta_dir = f"{AIRFLOW_HOME}/mirrors/{self.table_name}/data_deltas/"
        self.source_from_postgres = source_from_postgres
        self.source_from_local = source_from_local

        self.log_dir = f"{AIRFLOW_HOME}/logs/mirrors/"
        self.final_version_path = f"{AIRFLOW_HOME}/mirrors/{self.table_name}/mirr_{self.table_name}/"
        # self.spark = (SparkSession
        #     .builder
        #     .appName('create_mirror_md_account_d')
        #     .enableHiveSupport()
        #     .getOrCreate()
        # )

        builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.deltaTable = DeltaTable.forPath(self.spark, self.final_version_path)
        

    def save_to_log(self, log_row: tuple) -> None:
        """
        Добавляет новую запись в таблицу логов 
        создания зеркал в mirrors/logs/{table_name}
        row_to_table - строка с новой записью вида (delta_id, start_time, end_time, table_name)
        """
        schema = """
                delta_id BIGINT, 
                time_start DATETIME, 
                time_end DATETIME, 
                table_name STRING
                """
        if not os.path.isdir(self.log_dir):
            rows = [log_row]
            logs_table = self.spark.createDataFrame(rows, schema)
            logs_table.coalesce(1) \
                .write.format("csv")\
                .option("header", True) \
                .option("sep", ";") \
                .mode("overwrite") \
                .save(f"{self.log_dir}/")
        else:
            logs_table = self.spark.read.option("delimiter", ";") \
                .option("header", "true") \
                .csv(self.log_dir)
            newRow = self.spark.createDataFrame(log_row, schema)
            appended = logs_table.union(newRow)
            appended.coalesce(1) \
                .write.format("csv")\
                .option("header", True) \
                .option("sep", ";") \
                .mode("overwrite") \
                .save(f"{self.log_dir}/")


    def create_delta(self):
        """
        Сравнивает зеркало и таблицу источник,
        если есть обновления создает новую дельту
        и сохраняет в data_deltas
        """
        pass

    def make_final_mirror(self, first_delt_id_dir=None) -> None:
        """
        Сохраняет итоговое зеркало по обработанной делтьте.
        (используется в process_deltas)
        Если нет первоисточника (с чем сравнивать зеркало),
        то по умолчанию создается из первой дельты.
        """
        if not (self.source_from_postgres and self.source_from_local):
            if not first_delt_id_dir:
                os.copy(self.delta_dir + f'{first_delt_id_dir}/{self.table_name}.csv', self.final_version_path)
                return None

        self.deltaTable.coalesce(1) \
                .write.format("csv")\
                .option("header", True) \
                .option("sep", ";") \
                .mode("overwrite") \
                .save(f"{self.final_version_path}/")
        

    def process_deltas(self) -> None:
        """
        Обрабатывает все имеющиеся дельты в папке data_deltas 
        и сохраняет итоговый результат в mirr_md_acoount_d
        """
        tz_moscow = pytz.timezone("Europe/Moscow")
        
        #  1. Зайти в логи, посмотреть id полследней обработанной дельты 
        logs_table = self.spark.read.option("delimiter", ";") \
                .option("header", "true") \
                .csv(self.log_dir)
        last_delt_id_logs = logs_table.agg({"delta_id": "max"}).first()[0][0]

        #  2. Посомтреть id посследней дельты в папке 
        try:
            dirs_in_deltas = os.listdir(self.log_dir)
            dirs_in_deltas_int = [int(dirs_in_deltas[i]) for i in range(len(dirs_in_deltas))]
            first_delt_id_dir = min(dirs_in_deltas_int)
            last_delt_id_logs = last_delt_id_logs if last_delt_id_logs else first_delt_id_dir
            
            last_delt_id_dir = max(dirs_in_deltas_int)
        except Exception as e:
            last_delt_id_dir = 0
        

        # 3. Если появились новые дельты - обрабатываем их
        if  last_delt_id_logs < last_delt_id_dir:
            for i in range(last_delt_id_logs + 1, last_delt_id_dir + 1):
                time_start_load = datetime.now(tz_moscow)
                # Если нет зеркала, то создать на основе первой дельты 
                if last_delt_id_logs == first_delt_id_dir:
                    self.make_final_mirror(first_delt_id_dir=i)
                else: 
                    
                    
                    updatesDF = self.spark.read.option("delimiter", ";") \
                        .option("header", "true") \
                        .csv(f'{self.delta_dir}/{i}/')

                    # Генерация словаря для обнавления
                    dict_to_update = dict()
                    for field in updatesDF.schema.fields:
                        if field.name not in self.primary_key:
                            dict_to_update[field.name] = f'deltas.{field.name}'

                    # Генерация словаря для вставки 
                    dict_to_insert = dict()
                    for field in updatesDF.schema.fields:
                        dict_to_insert[field.name] = f'deltas.{field.name}'

                    # Обработать дельту + УСЛОВИЕ ПРИВЕСТИ К ОБЩЕМУ ВИДУ
                    self.deltaTable.alias("mirror").merge(
                        source = updatesDF.alias("deltas"),
                        condition = "mirror.ACCOUNT_RK = deltas.ACCOUNT_RK") \
                        .whenMatchedUpdate(set=dict_to_update) \
                        .whenNotMatchedInsert(values=dict_to_insert) \
                        .execute()

                    # Обновить зеркало 
                    self.make_final_mirror()
                
                # Записать в логи
                time_end_load = datetime.now(tz_moscow)
                log_row = (i, time_start_load, time_end_load, self.table_name)
                self.save_to_log(log_row)

mirr = Mirror(
    delta_dir=f"{AIRFLOW_HOME}/mirrors/md_account_d/data_deltas/",
    table_name='md_account_d',
    primary_key='ACCOUNT_RK'
)

mirr.process_deltas()