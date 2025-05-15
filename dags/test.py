from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'start_date': datetime(2024,11, 1),
    'retries': 0
}


with DAG('mydag', default_args=default_args, schedule_interval=None) as dag:
    
    move_files_from_landing_to_archive = BashOperator(
        task_id='updateuser'
        , bash_command='airflow users add-role -e adi.pratama@danamon.co.id -r Admin'
    )

#testing
