import airflow.utils.dates
from airflow.decorators import task
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowException
import os
import shutil

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(1),
 
}

with DAG(
    "03_copy_to_postgres",
    description="Responsible for creating and copying tables to postgres",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@once",
    default_args=default_args,
) as dag:

    # Poke for files in transformed folder
    poke_for_transformed_files = FileSensor(
        task_id="poke_for_transformed_files",
        filepath="data/transformed",
        fs_conn_id="fs_transformed",
        poke_interval=60 * 10,  # Vérifier toutes les 10 minutes
        timeout=24 * 60 * 60,
        retries=4,
        mode="reschedule",
        soft_fail=True,
        dag=dag,
    )

    # get all files from transformed folder
    def get_csv_filename(folder_path):
        csv_filenames = []
        for filename in os.listdir(folder_path):
            if filename.endswith(".csv"):
                csv_filenames.append(filename.split("_ds.csv")[0])
        return csv_filenames

    # def move_to_archive(filename):
    #     transformed_path = "data/transformed"
    #     archive_path = "data/archived"
    #     print("files are",os.listdir(transformed_path))
    #     try:
    #         shutil.move(os.path.join(transformed_path, f"{filename}.csv"), archive_path)
    #     except FileNotFoundError:
          
    #         raise AirflowException(f"File '{filename}.csv' not found in '{transformed_path}'")
    

    for filename in get_csv_filename("data/transformed"):

        

        # CREATE tables if exists
        create_tables = SQLExecuteQueryOperator(
            task_id=f"create_{filename}_task",
            sql=f"sql/create/{filename}.sql",
            split_statements=True,
            conn_id="postgres_db",
            return_last=False,
            dag=dag,
        )

        # Contrôler que le nombre de lignes a augmenter ?

        # COPY files to postgres database
        copy_tables = SQLExecuteQueryOperator(
            task_id=f"copy_{filename}_task",
            conn_id="postgres_db",
            sql=f"sql/copy/{filename}.sql",
            split_statements=True,
            return_last=False,
            dag=dag,
        )

        # Contrôler que le nombre de lignes a augmenter ?

        # Archive files after being copied with date as suffix
        # archive_transformed_data = PythonOperator(
        #     task_id=f"archive_transformed_data_task_{filename}",
        #     python_callable=move_to_archive,
        #     op_kwargs={"filename": filename},
        #     dag=dag,
        # )

        (
            poke_for_transformed_files
            >> create_tables
            >> copy_tables
            # >> archive_transformed_data
        )
