from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.cloud_storage_transfer_service import CloudDataTransferServiceJobStatusSensor
from airflow.providers.google.cloud.hooks.cloud_storage_transfer_service import CloudDataTransferServiceHook
import time

default_args = {
    'start_date': datetime(2024,11, 1),
    'retries': 0
}

AIRFLOW_VAR_DAP_LANDING_PROJECT_ID = Variable.get('AIRFLOW_VAR_DAP_LANDING_PROJECT_ID')

def sensor_sts(**kwargs):
    hook = CloudDataTransferServiceHook()
    STATUS = "PENDING"
    max_iterations = 120
    iteration_count = 0
    while STATUS != "SUCCESS" and iteration_count < max_iterations:
        print("Menunggu status menjadi SUCCESS...")
        time.sleep(60)
        iteration_count += 1
        var_status = hook.get_transfer_operation(operation_name=kwargs['transferOperations'])
        STATUS = var_status['metadata']['status']
        print("Status STS: " + var_status['metadata']['status'])
    kwargs['ti'].xcom_push(key='sensor_stat', value=var_status)


with DAG('test-sts', default_args=default_args, schedule_interval=None) as dag:
    
    # run_transfer_sensor = CloudDataTransferServiceJobStatusSensor(
    #         task_id='sensor',
    #         job_name="transferJobs-sts_dmu_workspace_grouping_channel_cleansing_fbi_initial-4481238265439189843",
    #         project_id=AIRFLOW_VAR_DAP_LANDING_PROJECT_ID,
    #         expected_statuses='SUCCESS',
    #         poke_interval=10
    #     )
    check_sts_sensor = PythonOperator(
            task_id='check_sensor',
            python_callable=sensor_sts,
            trigger_rule='all_done',
            provide_context=True,
            op_kwargs={'transferOperations' : "transferOperations/transferJobs-sts_dmu_workspace_grouping_channel_cleansing_fbi_initial-4481238265439189843"}
        )
    
    test_lanjutan = BashOperator(
            task_id='check_sensor_2',
            bash_command="""{% if task_instance.xcom_pull(task_ids='check_sensor',key='sensor_stat')['name'] is none %}
                            Nilai sensor tidak ditemukan.
                            {% else %}
                            Nilai sensor adalah: {{ task_instance.xcom_pull(task_ids='check_sensor',key='sensor_stat')['name'] }}
                            {% endif %}}
                        """
        )
    
    check_sts_sensor >> test_lanjutan