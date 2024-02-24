import airflow.utils.dates
from airflow.decorators import task
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowException
from airflow.models import Variable
from datetime import timedelta
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
    concurrency=20
) as dag:

   

    # get all files from transformed folder
    def get_csv_filename():
        import ast

        csv_filenames = ast.literal_eval(Variable.get("file_names"))
        return csv_filenames
      
 

    validate_dag = DummyOperator(
        task_id=f"validate_dag_task",
        trigger_rule="all_done",
        dag=dag,
    )

    for filename in get_csv_filename():
        # Poke for files in transformed folder
        poke_for_transformed_files = FileSensor(
            task_id=f"poke_for_transformed_{filename}",
            filepath=f"data/transformed/{filename}.csv",
            fs_conn_id="fs_transformed",
            poke_interval=30,  # Vérifier toutes les 10 minutes
            timeout=60,
            retries=0,
            mode="reschedule",
            soft_fail=True,
            dag=dag,
    )
        filename_short=filename.split('_ds')[0]
        # CREATE tables if exists
        create_tables = SQLExecuteQueryOperator(
            task_id=f"create_{filename_short}_task",
            sql=f"sql/create/{filename_short}.sql",
            split_statements=True,
            conn_id="postgres_db",
            return_last=False,
            dag=dag,
        )

        copy_tables = SQLExecuteQueryOperator(
            task_id=f"copy_{filename_short}_task",
            conn_id="postgres_db",
            sql=f"sql/copy/{filename_short}.sql",
            split_statements=True,
            return_last=False,
            sla=timedelta(minutes=10),
            dag=dag,
        )


        (
            poke_for_transformed_files
            >> create_tables
            >> copy_tables
            >> validate_dag
        )
