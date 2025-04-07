import pendulum
import time

from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator


with DAG(
    dag_id="external_dag_d",
    start_date=pendulum.datetime(2025, 3, 23, tz="UTC"),
    schedule="0 19 * * *",
    catchup=True,
    tags=["dag_d"],
) as child_dag:

    task_1 = PythonOperator(
        task_id="task_1",
        python_callable=lambda: time.sleep(10),
    )

    task_2 = PythonOperator(
        task_id="task_2",
        python_callable=lambda: time.sleep(100),
    )
    task_1  >> task_2
