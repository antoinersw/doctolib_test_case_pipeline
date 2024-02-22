import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.filesystem import FileSensor
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = DAG(
    dag_id="03_copy_to_postgres",
    description="Responsible for copying tables to postgres",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@once",
    
)

#Poke for files in processed folder
poke_for_processed_files = FileSensor(
        task_id='poke_for_processed_files',
        filepath='data/processed',
        fs_conn_id='fs_processed',
        poke_interval=60*10,  # Vérifier toutes les 10 minutes
        timeout=24*60*60,
        retries=4,
        mode='reschedule',
        soft_fail=True,
        dag=dag,
    )

#get all files from processed folder
def get_csv_filename(folder_path):
    csv_filenames = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.csv'):
            csv_filenames.append(filename.split('_ds.csv')[0])
    return csv_filenames

 
#get the name of all ds
#template the name of all ds in both the create table statement and the copy

for filename in get_csv_filename('data/processed') :

    #CREATE tables if exists
    create_tables = SQLExecuteQueryOperator(
        task_id =f'create_{filename}_task',
        sql=  f'sql/create/{filename}.sql',
        split_statements=True,
        conn_id='postgres_db', 
            return_last=False,
            dag=dag
    )

    #COPY files to postgres database
    copy_tables = SQLExecuteQueryOperator(
        task_id =f'copy_{filename}_task',
        conn_id='postgres_db',
        sql= f'sql/copy/{filename}.sql',
        split_statements=True,
            return_last=False,
            dag=dag
    )

    #Delete files after being copied
    
    poke_for_processed_files>>create_tables >>copy_tables