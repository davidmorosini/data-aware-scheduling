from datetime import datetime

from airflow.models.dag import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator


trigger_datasets = [
    Dataset("dag_a_event_1"),
    Dataset("dag_b_event_1")
]

with DAG(
    dag_id="dag_c",
    start_date=datetime(2025, 1, 1, 0, 0, 0),
    schedule=trigger_datasets,
    catchup=True,
):

    def process_data(*, inlet_events):
        for _dataset in trigger_datasets:
            print(f"Processing dataset: {_dataset}")
            events = inlet_events[_dataset]
            latest_event = events[-1].extra
            print("Latest Event: ", latest_event)

    my_task = PythonOperator(
        task_id="consumer_task",
        python_callable=process_data,
        inlets=trigger_datasets
    )
