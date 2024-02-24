from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.sensors.external_task import ExternalTaskSensor
import airflow.utils.dates
import csv
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable

import pandas as pd
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}


with DAG(
    "02_transform_files",
    description="Responsible for fetching the daily new data from multiple sources",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@daily",
    default_args=default_args,
) as dag:

    # Read the file and get the separator
    def get_separator(filename):
        # Read the first   lines of the file
        with open(filename, "r") as file:
            lines = [file.readline() for line in range(5)]
        for separator in [",", ";"]:
            if all(separator in line for line in lines):
                return separator
        return None

    def _transform_all_csv(**context):
        staging_folder_path = "data/staging"
        transform_folder_path = "data/transformed"
        # Get the list of csv files in the folder
        csv_files = [
            file for file in os.listdir(staging_folder_path) if file.endswith(".csv")
        ]
        # Print the content of each csv file
        for file in csv_files:
            staging_file_path = os.path.join(staging_folder_path, file)
            transform_file_path = os.path.join(transform_folder_path, file)
            df = pd.read_csv(staging_file_path,encoding='utf-8', sep=get_separator(staging_file_path),low_memory=False)
            # check # lines  > 0
            if df.empty:
                raise AirflowException(f"File : {file} contains 0 row !")
            # add the execution_date
            df["execution_date"] = context["execution_date"].strftime(
                "%Y-%m-%d %H:%M:%S"
            )
 
            df.insert(0, 'id', range(1, len(df) + 1))
            if 'Unnamed: 0' in df.columns:
                df.drop('Unnamed: 0', axis=1, inplace=True)
            df.to_csv(
                transform_file_path,
                sep=",",
                encoding="utf-8",
                quoting=csv.QUOTE_MINIMAL,
                index=False
            )

    def get_csv_filename():
        import ast
        csv_filenames = ast.literal_eval(Variable.get("file_names"))
        return csv_filenames

    for file_name in get_csv_filename():
        sense_files_dag_execution = FileSensor(
            task_id=f"poke_for_staged_{file_name}",
            filepath=f"data/staging/{file_name}.csv",
            fs_conn_id="fs_staging",
            poke_interval=60 * 10,  # Vérifier toutes les 10 minutes
            timeout=3600,
            retries=0,
            mode="reschedule",
            soft_fail=True,
            dag=dag,
        )
        transform_all_csv = PythonOperator(
            task_id=f"transform_{file_name}_task",
            python_callable=_transform_all_csv,
            dag=dag,
        )
        sense_files_dag_execution >> transform_all_csv
