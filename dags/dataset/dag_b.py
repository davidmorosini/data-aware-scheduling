from datetime import datetime

from airflow.models.dag import DAG
from airflow.datasets import Dataset
from airflow.datasets.metadata import Metadata
from airflow.operators.python import PythonOperator


TAG_NAME = "dag_b"
dataset_name_1 = f"{TAG_NAME}_event_1"
dataset_name_2 = f"{TAG_NAME}_event_2"
my_dataset_1 = Dataset(dataset_name_1)
my_dataset_2 = Dataset(dataset_name_2)


with DAG(
    dag_id=TAG_NAME,
    start_date=datetime(2025, 3, 1, 0, 0, 0),
    schedule="5 10,18 * * *",  # Every Day at 10:05
    catchup=True,
):

    def create_event_b1(**kwargs):
        """
        Popula o evento com informações personalizadas
        """
        _metadata = {
            "tag": TAG_NAME,
            "row_count": 35,
            "start_date": kwargs["data_interval_start"].to_iso8601_string(),
            "end_date": kwargs["data_interval_end"].to_iso8601_string(),
            "logical_date": kwargs["logical_date"].to_iso8601_string(),
            "nodash": kwargs["ts_nodash_with_tz"],
            "processing_type": "delsert",
        }
        print(f"Launch event ({dataset_name_1}): {_metadata}")
        yield Metadata(my_dataset_1, _metadata)
    
    def create_event_b2(**kwargs):
        """
        Popula o evento com informações personalizadas
        """
        _metadata = {
            "tag": TAG_NAME,
            "row_count": 35,
            "start_date": kwargs["data_interval_start"].to_iso8601_string(),
            "end_date": kwargs["data_interval_end"].to_iso8601_string(),
            "logical_date": kwargs["logical_date"].to_iso8601_string(),
            "nodash": kwargs["ts_nodash_with_tz"],
            "processing_type": "delsert",
        }
        print(f"Launch event ({dataset_name_2}): {_metadata}")
        yield Metadata(my_dataset_2, _metadata)

    my_task_1 = PythonOperator(
        task_id=dataset_name_1,
        python_callable=create_event_b1,
        outlets=[my_dataset_1]
    )

    my_task_2 = PythonOperator(
        task_id=dataset_name_2,
        python_callable=create_event_b2,
        outlets=[my_dataset_2]
    )
