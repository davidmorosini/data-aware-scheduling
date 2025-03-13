from datetime import datetime

from airflow.models.dag import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable


trigger_dataset = Dataset("dag_b_event_1")

with DAG(
    dag_id="dag_d",
    start_date=datetime(2025, 1, 1, 0, 0, 0),
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("*/5 * * * *", timezone="UTC"),
        datasets=trigger_dataset
    ),
    catchup=True,
):

    def process_data(*, inlet_events):
        events_tag_b = inlet_events[trigger_dataset]
        latest_tag_b_event = events_tag_b[-1].extra
        print("Latest Event: ", latest_tag_b_event)

    my_task = PythonOperator(
        task_id="consumer_task",
        python_callable=process_data,
        inlets=[trigger_dataset]
    )
