# Aggréger des tables
import airflow.utils.dates
from airflow.decorators import task
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.filesystem import FileSensor
from airflow.exceptions import AirflowException
 


# requêter les tables
#
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(1),
}


with DAG(
    "04_build_datamart",
    description="Responsible for building aggregated tables in postgres",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@once",
    default_args=default_args,
) as dag:

    # create a branch operator => If the datamart has been initiated then skip the database creation and the dim date creation
    create_dim_date = SQLExecuteQueryOperator(
        task_id=f"create_dim_date_task",
        sql=f"sql/create/dim_date.sql",
        split_statements=True,
        conn_id="postgres_db",
        return_last=False,
        dag=dag,
    )

    create_overload_appointment_monitoring = SQLExecuteQueryOperator(
        task_id=f"create_overload_appointment_monitoring_task",
        sql=f"sql/create/datamart/overload_appointment_monitoring.sql",
        split_statements=True,
        conn_id="postgres_db",
        return_last=False,
        dag=dag,
    )

    create_aggreg_count_vax = SQLExecuteQueryOperator(
        task_id=f"create_aggreg_count_vax_task",
        sql=f"sql/create/datamart/aggreg_count_vax.sql",
        split_statements=True,
        conn_id="postgres_db",
        return_last=False,
        dag=dag,
    )

    refresh_powerbi_dataset = DummyOperator(
        task_id='refresh_powerbi_dataset_task'
        ,dag=dag
        # At the end of the pipeline we could add a task or another DAG to trigger power bi dataset to refresh. This would guarantee great SLAs !
        # Since I wont host that docker implementation of that pipeline I won't implement that hook
        # https://github.com/christo-olivier/airflow_powerbi_plugin/blob/master/powerbi_plugin/hooks/powerbi_hook.py
    )
  

    create_dim_date >> create_overload_appointment_monitoring >> create_aggreg_count_vax >> refresh_powerbi_dataset
