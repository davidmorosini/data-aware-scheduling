from datetime import datetime

from airflow.models.dag import DAG
from airflow.datasets import Dataset
from airflow.datasets.metadata import Metadata
from airflow.operators.python import PythonOperator


TAG_NAME = "dag_a"
dataset_name = f"{TAG_NAME}_event_1"
my_dataset = Dataset(dataset_name)


with DAG(
    dag_id=TAG_NAME,
    start_date=datetime(2025, 3, 1, 0, 0, 0),
    schedule="0 10 * * *",  # Every Day at 10:00
    catchup=True,
):

    def create_event_a1(**kwargs):
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
        print(f"Launch event ({dataset_name}): {_metadata}")
        yield Metadata(my_dataset, _metadata)

    my_task = PythonOperator(
        task_id=dataset_name,
        python_callable=create_event_a1,
        outlets=[my_dataset]
    )
