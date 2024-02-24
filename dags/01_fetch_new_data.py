from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import airflow.utils.dates
import csv
import chardet
import hashlib
import requests
from airflow.models import XCom
from airflow.exceptions import AirflowException
import pandas as pd
import os
from io import StringIO

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    "01_fetch_new_data",
    description="Responsible for fetching the daily new data from multiple sources",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@daily",
    default_args=default_args,
    concurrency=20
) as dag:

    ###################
    # Stored Data source - Are init each time the docker run
    ###################
    appointments_by_center_ds = Variable.get("appointments_by_center_ds")
    vaccination_centers_ds = Variable.get("vaccination_centers_ds")
    vaccination_stock_ds = Variable.get("vaccination_stock_ds")
    vaccination_vs_appointment_ds = Variable.get("vaccination_vs_appointment_ds")
    geo_etendue_ds= Variable.get('geo_etendue')

    ############
    # Stored Hash - Are stored with value == True to ease the first run
    ############
    previous_hash_vaccination_vs_appointment_ds =True # Variable.get(
       #  "previous_hash_vaccination_vs_appointment_ds"
     #)
    previous_hash_vaccination_centers_ds = True #Variable.get(
      #   "previous_hash_vaccination_centers_ds"
   #  )
    previous_hash_appointments_by_center_ds = True #Variable.get(
    #     "previous_hash_appointments_by_center_ds"
   #  )
    previous_hash_vaccination_stock_ds = True #Variable.get(
    #     "previous_hash_vaccination_stock_ds"
    # )

    def get_separator(file):
        # Read the first lines of the file
        lines = file.splitlines()
        for separator in [",", ";"]:
            if all(separator in line for line in lines):
                return separator
        return None


    def verify_hash(csv_text, previous_hash):
        
         
        csv_hash = hashlib.md5(csv_text.encode()).hexdigest()  # Encode the string before hashing

        return previous_hash == csv_hash
    
    def _fetch_data(**context):
        # Function to retrieve data from the CSV file
        csv_url = context["csv_url"]
        header = {"Content-Type": "text/csv","charset":"utf-8"}
        try:
            response = requests.get(csv_url, headers=header)
 
            csv_content = response.content
            encoding = chardet.detect(csv_content)["encoding"]
            csv_text = response.text

            # Write the content to a file in a specific folder
            folder_path = "data/staging"
         
            file_name = context["file_name"] + ".csv"
            file_path = os.path.join(folder_path, file_name)

            # Create a file-like object from the CSV content string
            csv_file = StringIO(csv_text)

            if verify_hash(csv_text,previous_hash) :
                return False
            # Read the CSV content using pandas
            df = pd.read_csv(
                csv_file,
                sep=get_separator(csv_text),
                encoding=encoding,
                on_bad_lines="skip",
            )

            # Write the DataFrame to a CSV file
            df.to_csv(file_path, encoding="utf-8", sep=",", quoting=csv.QUOTE_MINIMAL)

        except (requests.exceptions.RequestException, UnicodeDecodeError) as e:
            # Raise an exception with the error message
            raise AirflowException(f"Failed to fetch data from {csv_url}: {str(e)}")

  


 



    for csv_url, previous_hash, file_name in [
        (
            appointments_by_center_ds,
            previous_hash_appointments_by_center_ds,
            "appointments_by_center_ds",
        ),
        (
            vaccination_centers_ds,
            previous_hash_vaccination_centers_ds,
            "vaccination_centers_ds",
        ),
        (
            vaccination_stock_ds,
            previous_hash_vaccination_stock_ds,
            "vaccination_stock_ds",
        ),
        (
            vaccination_vs_appointment_ds,
            previous_hash_vaccination_vs_appointment_ds,
            "vaccination_vs_appointment_ds",
        ),
         (
            geo_etendue_ds,
            True,
            "geo_etendue_ds",
        ),
    ]:
        fetch_data_task = PythonOperator(
            task_id=f"fetch_data_{file_name}",
            python_callable=_fetch_data,
            op_kwargs={"csv_url": csv_url, "file_name": file_name,"previous_hash": previous_hash},
            retries=1,
            sla=timedelta(minutes=5),
            dag=dag,
        )

        # verify_hash_task = PythonOperator(
        #     task_id=f"verify_hash_{file_name}",
        #     python_callable=_verify_hash,
        #     op_kwargs={"previous_hash": previous_hash, "file_name": file_name},
        #     retries=0,
        #     dag=dag,
        # )
            # Créer une tâche pour envoyer un email si la tâche ensure_success ne se termine pas avec succès
        # https://hevodata.com/learn/airflow-emailoperator/
        send_email_on_fail = EmailOperator(
            conn_id=None,  # TODO
            task_id=f"send_email_on_fail_{file_name}_task",
            to="antoine.rsw@gmail.com",
            subject="[FAILURE] Data source could not be retrieved",
            html_content=" Error occured on {{ds}}. Please check the logs for more information.",
            trigger_rule="one_failed",
        
            dag=dag,
        )

        fetch_data_task >>  send_email_on_fail 
