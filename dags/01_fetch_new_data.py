from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import csv
import pathlib
import hashlib
import requests
from airflow.models import XCom
from airflow.exceptions import AirflowException
import pandas as pd
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    "01_fetch_new_data",
    description="Responsible for fetching the daily new data from multiple sources",
    start_date=  datetime(2024,2,22),
    schedule_interval='@daily',
    default_args=default_args,
) as dag:

        # For each csv file I need to :
        # - Query the csv
        # - Verify if the hash if either "first time" or it is a different csv hash than yesterday
        #   - if so continue
        #   - if no send an alert
        # - Store the data somewhere

        ###################
        # Stored Data source - Are init each time the docker run
        ###################
        appointments_by_center_ds = Variable.get("appointments_by_center_ds")
        vaccination_centers_ds = Variable.get("vaccination_centers_ds")
        vaccination_stock_ds = Variable.get("vaccination_stock_ds")
        vaccination_vs_appointment_ds = Variable.get("vaccination_vs_appointment_ds")

        ############
        # Stored Hash - Are stored with value == True to ease the first run
        ############
        previous_hash_vaccination_vs_appointment_ds = Variable.get(
            "previous_hash_vaccination_vs_appointment_ds"
        )
        previous_hash_vaccination_centers_ds = Variable.get(
            "previous_hash_vaccination_centers_ds"
        )
        previous_hash_appointments_by_center_ds = Variable.get(
            "previous_hash_appointments_by_center_ds"
        )
        previous_hash_vaccination_stock_ds = Variable.get(
            "previous_hash_vaccination_stock_ds"
        )


        def _fetch_data(**context):
            # Function to retrieve data from the CSV file
            csv_url = context["csv_url"]
            header = {"Content-Type": "text/csv; charset=utf-8"}
            try:
                response = requests.get(csv_url, headers=header)
                response.encoding = "utf-8"
                csv_content = response.text

                # Write the content to a file in a specific folder
                folder_path = "data/staging"
                file_name = context["file_name"] + ".csv"
                file_path = os.path.join(folder_path, file_name)
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(csv_content)

            except requests.exceptions.RequestException as e:
                # If there is an error, the XCom variable is incremented
                task_id = context["task_instance"].task_id
                current_count = XCom.get_one(
                    key=f"failed_task_count_{task_id}", execution_date=context["execution_date"]
                )
                if current_count is None:
                    current_count = 0
                new_count = current_count + 1
                XCom.set(
                    key=f"failed_task_count_{task_id}",
                    value=new_count,
                    execution_date=context["execution_date"],
                )

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
                csv_data.encode()
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
            subject="[NOTIFICATION] Data source could not be retrieved",
            html_content="Please check the logs for more information.",
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
        ]:
            fetch_data_task = PythonOperator(
                task_id=f"fetch_data_{file_name}",
                python_callable=_fetch_data,
                op_kwargs={"csv_url": csv_url, "file_name": file_name},
                retries=2,
                dag=dag,
            )

            verify_hash_task = PythonOperator(
                task_id=f"verify_hash_{file_name}",
                python_callable=_verify_hash,
                op_kwargs={"previous_hash": previous_hash, "file_name": file_name},
                provide_context=True,
                retries=0,
                dag=dag,
            )

            #fetch_data_task >> 
            verify_hash_task >>  ensure_success >> send_email_on_fail