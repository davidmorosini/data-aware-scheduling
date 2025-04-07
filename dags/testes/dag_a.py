from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.datasets import Dataset

my_dataset = Dataset("dataset_name")


def is_reprocessing(**kwargs) -> bool:
    """
    Verifica se a Execução atual é uma execução agendada ou um reprocessamento.
    O reprocessamento é caracterizado neste caso como uma execução manual da DAG,
    inclusive um clear em uma DagRun previamente executada.
    """
    dag_run = kwargs["dag_run"]
    if dag_run.run_type == "manual" or dag_run.clear_number > 0:
        return True
    
    return False

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 4),
    'retries': 1,
}

with DAG('check_execution_type_dag',
         default_args=default_args,
         schedule_interval='*/5 * * * *',
         catchup=False) as dag:

    check_task = PythonOperator(
        task_id='check_execution_type',
        python_callable=is_reprocessing,
        provide_context=True,
        outlets=[my_dataset]
    )

    check_task
