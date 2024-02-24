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

    def _fetch_data(**context):
        # Function to retrieve data from the CSV file
        csv_url = context["csv_url"]
        header = {"Content-Type": "text/csv"}
        try:
            response = requests.get(csv_url, headers=header)
            response.raise_for_status()  # Raise an exception if the request was unsuccessful
            csv_content = response.content
            encoding = chardet.detect(csv_content)["encoding"]
            csv_text = response.text

            # Write the content to a file in a specific folder
            folder_path = "data/staging"
            os.makedirs(
                folder_path, exist_ok=True
            )  # Create the folder if it doesn't exist
            file_name = context["file_name"] + ".csv"
            file_path = os.path.join(folder_path, file_name)

            # Create a file-like object from the CSV content string
            csv_file = StringIO(csv_text)

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

    def _verify_hash(**context):
        # Function to compare the hash of the retrieved data with the previous hash
        folder_path = "data/staging"
        file_name = context["file_name"] + ".csv"
        file_path = os.path.join(folder_path, file_name)
        with open(file_path, "r") as f:
            csv_data = f.read()  # Use f.read() to read the content of the file
        csv_hash = hashlib.md5(
            csv_data.encode(encoding="utf-8")
        ).hexdigest()  # Encode the string before hashing
        previous_hash = context["previous_hash"]

        if previous_hash == csv_hash:
            # The hash is the same as the previous hash
            raise AirflowException("No changes detected")
        else:
            # The hash is either "first time" or it is different from the previous hash
            previous_hash = Variable.set(
                f"previous_hash_{context['file_name']}", csv_hash
            )
            return "new_data"

    ensure_success = DummyOperator(
        task_id="ensure_success_task", trigger_rule="all_success", dag=dag
    )

    # Créer une tâche pour envoyer un email si la tâche ensure_success ne se termine pas avec succès
    # https://hevodata.com/learn/airflow-emailoperator/
    send_email_on_fail = EmailOperator(
        conn_id=None,  # TODO
        task_id="send_email_on_fail_task",
        to="antoine.rsw@gmail.com",
        subject="[FAILURE] Data source could not be retrieved",
        html_content=" Error occured on {{ds}}. Please check the logs for more information.",
        trigger_rule="one_failed",
    
        dag=dag,
    )

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
            op_kwargs={"csv_url": csv_url, "file_name": file_name},
            retries=1,
            sla=timedelta(minutes=5),
            dag=dag,
        )

        verify_hash_task = PythonOperator(
            task_id=f"verify_hash_{file_name}",
            python_callable=_verify_hash,
            op_kwargs={"previous_hash": previous_hash, "file_name": file_name},
            retries=0,
            dag=dag,
        )

        fetch_data_task >> verify_hash_task >> ensure_success >> send_email_on_fail
