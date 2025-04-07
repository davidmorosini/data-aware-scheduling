from datetime import datetime

from airflow.models.dag import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable


trigger_dataset = Dataset("dataset_name")

with DAG(
    dag_id="consume_execution_type_dag",
    start_date=datetime(2025, 1, 1, 0, 0, 0),
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("*/5 * * * *", timezone="UTC"),
        datasets=trigger_dataset
    ),
    catchup=False,
):

    def process_data(inlet_events, **kwargs):
        events = inlet_events[trigger_dataset]
        print(events[-1])

        dag_run = kwargs["dag_run"]
        print(dag_run.__dict__)



    my_task = PythonOperator(
        task_id="consumer_task",
        python_callable=process_data,
        inlets=[trigger_dataset]
    )
