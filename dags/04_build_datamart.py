# Aggréger des tables
import airflow.utils.dates
from airflow import DAG
from datetime import timedelta
from airflow.operators.dummy import DummyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor

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
    schedule_interval="@daily",
    default_args=default_args,
    concurrency=10,
) as dag:

    ensure_database_is_loaded = ExternalTaskSensor(
        task_id="sense_previous_dag_execution_task",
        external_dag_id="03_copy_to_postgres",
        external_task_id="validate_dag_task",
        timeout=60 * 10,
        sla=timedelta(minutes=10),
        dag=dag,
    )

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

    create_dim_centre = SQLExecuteQueryOperator(
        task_id=f"create_dim_centre_task",
        sql=f"sql/create/datamart/dim_centre.sql",
        split_statements=True,
        conn_id="postgres_db",
        return_last=False,
        dag=dag,
    )

    create_dim_geo = SQLExecuteQueryOperator(
        task_id=f"create_dim_geo_task",
        sql=f"sql/create/datamart/dim_geo.sql",
        split_statements=True,
        conn_id="postgres_db",
        return_last=False,
        dag=dag,
    )

    refresh_powerbi_dataset = DummyOperator(
        task_id="refresh_powerbi_dataset_task",
        # retries:3
        # sla=timedelta(minutes=15),
        
        dag=dag,
        # At the end of the pipeline we could add a task or another DAG to trigger power bi dataset to refresh. This would guarantee better SLAs than refreshing the report periodically from the service !
        # Since I wont host that docker implementation of that pipeline I won't implement that hook
        # https://github.com/christo-olivier/airflow_powerbi_plugin/blob/master/powerbi_plugin/hooks/powerbi_hook.py
    )

    (
    ensure_database_is_loaded
    >> [create_dim_date
    , create_overload_appointment_monitoring
    , create_aggreg_count_vax
    , create_dim_centre
    , create_dim_geo]  >> refresh_powerbi_dataset
)

 
