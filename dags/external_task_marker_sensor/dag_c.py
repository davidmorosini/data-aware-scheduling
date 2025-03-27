import pendulum
from datetime import timedelta
import time

from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor


with DAG(
    dag_id="external_dag_c",
    start_date=pendulum.datetime(2025, 3, 23, tz="UTC"),
    schedule="0 18 * * *",
    catchup=True,
    tags=["dag_c"],
) as child_dag:

    sensor_dag_a = ExternalTaskSensor(
        task_id="sensor_dag_a",
        external_dag_id="external_dag_a",
        external_task_ids=["task_1", "task_3"],
        timeout=600,
        poll_interval=10,
        execution_delta=timedelta(hours=1),
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        deferrable=True,
    )

    sensor_dag_b = ExternalTaskSensor(
        task_id="sensor_dag_b",
        external_dag_id="external_dag_b",
        external_task_ids=["ets_dag_c"],
        timeout=600,
        poll_interval=10,
        execution_delta=timedelta(hours=1, minutes=30),
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        deferrable=True,
    )

    task = PythonOperator(
        task_id="task",
        python_callable=lambda: time.sleep(10),
    )
    [sensor_dag_a, sensor_dag_b]  >> task
