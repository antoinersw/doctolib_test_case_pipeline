import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
import csv
import requests
import requests.exceptions as requests_exceptions
import pandas as pd
import os
import chardet

 
with DAG(
    dag_id="02_transform_files",
    description="Responsible for transforming data from staging folder",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@once",
 
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

    #
    def _process_all_csv():
        staging_folder_path = "data/staging"
        process_folder_path = "data/transformed"
        # Get the list of csv files in the folder
        csv_files = [
            file for file in os.listdir(staging_folder_path) if file.endswith(".csv")
        ]
        # Print the content of each csv file
        for file in csv_files:
            staging_file_path = os.path.join(staging_folder_path, file)
            process_file_path = os.path.join(process_folder_path, file)
            df = pd.read_csv(staging_file_path, sep=get_separator(staging_file_path))
            df.to_csv(
                process_file_path, sep=",", encoding="utf-8", quoting=csv.QUOTE_STRINGS
            )

    # Listen to the external task from DAG 01
    sense_previous_dag_execution = ExternalTaskSensor(
        task_id="sense_previous_dag_execution_task",
        external_dag_id="01_fetch_new_data",
        external_task_id="ensure_success_task",
        dag=dag,
    )

    # Normalize the csv and move it to the transformed folder
    # TODO Améliorer largement le process de cleaning
    process_all_csv = PythonOperator(
        task_id="process_all_csv_task",
        python_callable=_process_all_csv,
        dag=dag,
    )

    # Verifier que l'on a tous les fichiers que l'on veut

    sense_previous_dag_execution >> process_all_csv
