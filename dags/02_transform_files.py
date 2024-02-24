from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.sensors.external_task import ExternalTaskSensor
import airflow.utils.dates
import csv


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
            df = pd.read_csv(staging_file_path, sep=get_separator(staging_file_path))
            # check # lines  > 0
            if df.count() > 0:
                raise AirflowException(f"File {file} contains 0 row !")
            # add the execution_date
            df["execution_date"] = context["execution_date"].strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            df.rename(columns={df.columns[0]: "id"}, inplace=True)

            df.to_csv(
                transform_file_path,
                sep=",",
                encoding="utf-8",
                quoting=csv.QUOTE_MINIMAL,
            )

    # Listen to the external task from DAG 01 and trigger on success
    sense_previous_dag_execution = ExternalTaskSensor(
        task_id="sense_previous_dag_execution_task",
        external_dag_id="01_fetch_new_data",
        external_task_id="ensure_success_task",
        timeout=3600,
        dag=dag,
    )

    # Normalize the csv and move it to the transformed folder
    # TODO Améliorer largement le process de cleaning
    transform_all_csv = PythonOperator(
        task_id="transform_all_csv_task",
        python_callable=_transform_all_csv,
        dag=dag,
    )

    # Verifier que l'on a tous les fichiers que l'on veut

    sense_previous_dag_execution >> transform_all_csv
