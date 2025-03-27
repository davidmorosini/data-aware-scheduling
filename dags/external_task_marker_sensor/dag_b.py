import pendulum
import time

from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskMarker


with DAG(
    dag_id="external_dag_b",
    start_date=pendulum.datetime(2025, 3, 23, tz="UTC"),
    catchup=True,
    schedule="30 16 * * *",
    tags=["dag_b"],
) as parent_dag:
    
    task_1 = PythonOperator(
        task_id="task_1",
        python_callable=lambda: time.sleep(10),
    )

    etm_dag_c = ExternalTaskMarker(
        task_id="etm_dag_c",
        external_dag_id="external_dag_c",
        external_task_id="ets_dag_b",
        execution_date="{{ (logical_date + macros.timedelta(hours=1, minutes=30)).isoformat() }}"
    )

    etm_dag_d = ExternalTaskMarker(
        task_id="etm_dag_d",
        external_dag_id="external_dag_d",
        external_task_id="ets_dag_b",
        execution_date="{{ (logical_date + macros.timedelta(hours=2, minutes=30)).isoformat() }}"
    )
    task_1 >> [etm_dag_c, etm_dag_d]
