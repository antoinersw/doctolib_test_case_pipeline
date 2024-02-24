import airflow.utils.dates
from airflow.decorators import task
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.exceptions import AirflowException
from airflow.models import Variable
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
    schedule_interval="@daily",
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
    def get_csv_filename():
        import ast
        csv_filenames =  ast.literal_eval(Variable.get('file_names'))
        return csv_filenames


    def _move_to_archive(**context):
        transformed_path = "data/transformed"
        archive_path = "data/archived"
        today_date = context['execution_date'].strftime('%Y-%m-%d')
        
        # Get the filename from the context
        filename = f'{context["filename"]}_ds.csv'
       
 
        # Construct the new filename with today's date
        new_filename = f'{filename.split(".csv")[0]}_{today_date}.csv'
 
         
        try:
        # Move the file to the archive directory with the new filename
            shutil.move(f'{transformed_path}/{filename}', f'{archive_path}/{new_filename}')
        except FileNotFoundError:

            raise AirflowException(f"{filename} not found in {transformed_path}")
 
    validate_dag = DummyOperator(
            task_id=f"validate_dag_task",
            trigger_rule="all_success",
            dag=dag,
        )
    
   
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
    
        copy_tables = SQLExecuteQueryOperator(
            task_id=f"copy_{filename}_task",
            conn_id="postgres_db",
            sql=f"sql/copy/{filename}.sql",
            split_statements=True,
            return_last=False,
            dag=dag,
        )
        archive_transformed_data = PythonOperator(
            task_id=f"archive_transformed_data_task_{filename}",
            python_callable=_move_to_archive,
            op_kwargs={"filename":filename},
            dag=dag,
        )

        
        (
            poke_for_transformed_files
            >> create_tables
            >> copy_tables
            >> archive_transformed_data
            >> validate_dag
        )

   
