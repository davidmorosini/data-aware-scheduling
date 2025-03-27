import pendulum
import time

from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskMarker


with DAG(
    dag_id="external_dag_a",
    start_date=pendulum.datetime(2025, 3, 23, tz="UTC"),
    catchup=True,
    schedule="0 17 * * *",
    tags=["dag_a"],
) as parent_dag:
    
    task_1 = PythonOperator(
        task_id="task_1",
        python_callable=lambda: time.sleep(10),
    )
    
    task_2 = PythonOperator(
        task_id="task_2",
        python_callable=lambda: time.sleep(10),
    )
    
    task_3 = PythonOperator(
        task_id="task_3",
        python_callable=lambda: time.sleep(10),
    )

    etm_dag_c = ExternalTaskMarker(
        task_id="etm_dag_c",
        external_dag_id="external_dag_c",
        external_task_id="task_1",
        execution_date="{{ (logical_date + macros.timedelta(hours=1)).isoformat() }}"
    )
    task_1 >> etm_dag_c
    task_2 >> task_3 >> etm_dag_c
