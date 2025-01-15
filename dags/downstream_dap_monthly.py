from airflow import settings
from datetime import datetime, timedelta, time
from airflow import DAG
from functools import partial
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import (BigQueryInsertJobOperator , BigQueryValueCheckOperator)
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceRunJobOperator
from airflow.providers.google.cloud.sensors.cloud_storage_transfer_service import CloudDataTransferServiceJobStatusSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from DATA.utils.dap.load_config import ConfigFile
from DATA.operator.dap.function_downstream_dap_monthly import Function

import re
import os
import pendulum
import pytz

local_tz = pendulum.timezone("Asia/Jakarta")

_BASE_FOLDER = settings.DAGS_FOLDER
timeout_treshold = timedelta(minutes=5)


#Initial Variable
AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID = Variable.get('AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID')
AIRFLOW_VAR_DAP_LANDING_PROJECT_ID = Variable.get('AIRFLOW_VAR_DAP_LANDING_PROJECT_ID')
AIRFLOW_VAR_DAP_LANDING_BUCKET = Variable.get('AIRFLOW_VAR_DAP_LANDING_BUCKET')
AIRFLOW_VAR_DAP_ARCHIVED_BUCKET = Variable.get('AIRFLOW_VAR_DAP_ARCHIVED_BUCKET')
AIRFLOW_VAR_DAP_OMT_MASTER_JOB = Variable.get('AIRFLOW_VAR_DAP_OMT_MASTER_JOB')
AIRFLOW_VAR_DAP_CONNECTION_BIGLAKE = Variable.get('AIRFLOW_VAR_DAP_CONNECTION_BIGLAKE')
AIRFLOW_VAR_DAP_EXPORT_FORMAT = Variable.get('AIRFLOW_VAR_DAP_EXPORT_FORMAT')
AIRFLOW_VAR_DAP_OMT_PROCESS_LOG = Variable.get('AIRFLOW_VAR_DAP_OMT_PROCESS_LOG')
AIRFLOW_VAR_DAP_OMT_ERROR_LOG = Variable.get('AIRFLOW_VAR_DAP_OMT_ERROR_LOG')
AIRFLOW_VAR_DAP_OMT_JOB_DEPENDENCY = Variable.get('AIRFLOW_VAR_DAP_OMT_JOB_DEPENDENCY')
AIRFLOW_VAR_DAP_OMT_CALENDAR_DATE = Variable.get('AIRFLOW_VAR_DAP_OMT_CALENDAR_DATE')
AIRFLOW_VAR_DAP_OMT_AUTOFILL_BACKLOG_RETENTION = Variable.get('AIRFLOW_VAR_DAP_OMT_AUTOFILL_BACKLOG_RETENTION')
AIRFLOW_VAR_DAP_OMT_CHECK_DT_IN_MONTH = Variable.get('AIRFLOW_VAR_DAP_OMT_CHECK_DT_IN_MONTH')
AIRFLOW_VAR_DAP_MIN_DATE_TRANSACTION_INITIAL = Variable.get('AIRFLOW_VAR_DAP_MIN_DATE_TRANSACTION_INITIAL')
AIRFLOW_VAR_DAP_RETRY_DELAY = timedelta(minutes= int(Variable.get('AIRFLOW_VAR_DAP_RETRY_DELAY')) )   
AIRFLOW_VAR_DAP_LOCATION = Variable.get('AIRFLOW_VAR_DAP_LOCATION')
AIRFLOW_VAR_DAP_EMAIL_SEND_TO = Variable.get('AIRFLOW_VAR_DAP_EMAIL_SEND_TO')
AIRFLOW_VAR_DAP_RETRY_TARGET_TIME_LIMIT_HOUR = int(Variable.get('AIRFLOW_VAR_DAP_RETRY_TARGET_TIME_LIMIT_HOUR'))

AIRFLOW_VAR_DAP_DATASET_DM = Variable.get('AIRFLOW_VAR_DAP_DATASET_DM')
AIRFLOW_VAR_DAP_DATASET_DM_MASKED = Variable.get('AIRFLOW_VAR_DAP_DATASET_DM_MASKED')
AIRFLOW_VAR_DAP_DATASET_DW = Variable.get('AIRFLOW_VAR_DAP_DATASET_DW')
AIRFLOW_VAR_DAP_DATASET_MISPLUS = Variable.get('AIRFLOW_VAR_DAP_DATASET_MISPLUS')
AIRFLOW_VAR_DAP_DATASET_SP = Variable.get('AIRFLOW_VAR_DAP_DATASET_SP')
AIRFLOW_VAR_DAP_DATASET_TEMP = Variable.get('AIRFLOW_VAR_DAP_DATASET_TEMP')
AIRFLOW_VAR_DAP_DATASET_TEMP_CLEANSING = Variable.get('AIRFLOW_VAR_DAP_DATASET_TEMP_CLEANSING')
AIRFLOW_VAR_DAP_DATASET_TEMP_SP	= Variable.get('AIRFLOW_VAR_DAP_DATASET_TEMP_SP')
AIRFLOW_VAR_DAP_DATASET_UDF	= Variable.get('AIRFLOW_VAR_DAP_DATASET_UDF')

AIRFLOW_VAR_DAP_JOB_TYPE = 'downstream_dap_monthly'
AIRFLOW_VAR_DAP_DAG_ID = "downstream_dap_monthly"
AIRFLOW_VAR_DAP_DAG_DESCRIPTION = "Parent Job for Downstream Job DAP Monthly"
AIRFLOW_VAR_DAP_DAG_ALIAS = 'downstream_dap_monthly'
AIRFLOW_VAR_DAP_DAG_PATH = '/DATA/config/dap/downstream_dap_monthly/'

# Calculate Retries for Check Dependency Datamart , and store it in AIRFLOW_VAR_DAP_RETRIES variable
current_time = datetime.now(tz=local_tz).time()
target_time = time(hour=AIRFLOW_VAR_DAP_RETRY_TARGET_TIME_LIMIT_HOUR, minute=0, second=0)
diff = datetime.combine(datetime.min, target_time) - datetime.combine(datetime.min, current_time)
Retries = int(diff/AIRFLOW_VAR_DAP_RETRY_DELAY)
print(Retries)
AIRFLOW_VAR_DAP_RETRIES = Retries 

#Yaml Config Path Folder
yaml_configs = ConfigFile.load_configurations(f'{_BASE_FOLDER}'+ AIRFLOW_VAR_DAP_DAG_PATH +'*.yaml')

function_master = Function(
                           AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID
                           ,AIRFLOW_VAR_DAP_LANDING_PROJECT_ID
                           ,AIRFLOW_VAR_DAP_LANDING_BUCKET
                           ,AIRFLOW_VAR_DAP_ARCHIVED_BUCKET
                          , AIRFLOW_VAR_DAP_EXPORT_FORMAT
                          , AIRFLOW_VAR_DAP_OMT_PROCESS_LOG
                          , AIRFLOW_VAR_DAP_OMT_ERROR_LOG
                          , AIRFLOW_VAR_DAP_OMT_JOB_DEPENDENCY
                          , AIRFLOW_VAR_DAP_OMT_CALENDAR_DATE
                          , AIRFLOW_VAR_DAP_OMT_AUTOFILL_BACKLOG_RETENTION
                          , AIRFLOW_VAR_DAP_LOCATION
                          , AIRFLOW_VAR_DAP_EMAIL_SEND_TO
                          , AIRFLOW_VAR_DAP_JOB_TYPE
                          , AIRFLOW_VAR_DAP_DAG_ID
                          , AIRFLOW_VAR_DAP_DAG_DESCRIPTION
                          , AIRFLOW_VAR_DAP_DAG_ALIAS
                          , AIRFLOW_VAR_DAP_DAG_PATH
                           )
default_args = {
    'start_date': datetime(2000, 1, 1, 1, 1, tzinfo=local_tz)
    , 'retries': 0
}

_BASE_FOLDER = settings.DAGS_FOLDER
timeout_treshold = timedelta(minutes=2)


with DAG(AIRFLOW_VAR_DAP_DAG_ID
    , default_args=default_args
    , description=AIRFLOW_VAR_DAP_DAG_DESCRIPTION
    , schedule_interval=None
    , catchup=False
    , tags=["DOWNSTREAM","MONTHLY","SEQ_MAIN","DAP"]) as AIRFLOW_VAR_DAP_DAG_ALIAS:
    
    Start = EmptyOperator(task_id="Start")

    t1 = TriggerDagRunOperator(
        task_id='dw_dap_fact_monthly'
        , trigger_dag_id='dw_dap_fact_monthly'
        , execution_date =  "{{ next_execution_date.in_timezone('Asia/Jakarta') }}'"  
        , wait_for_completion=True
        , reset_dag_run=True
        , retries = 2
    )

    t2 = TriggerDagRunOperator(
        task_id='dm_stg_dap_monthly'
        , trigger_dag_id='dm_stg_dap_monthly'
        , execution_date =  "{{ next_execution_date.in_timezone('Asia/Jakarta') }}'"  
        , wait_for_completion=True
        , reset_dag_run=True
        , retries = 2
    )

    t3 = TriggerDagRunOperator(
        task_id='dm_dap_monthly'
        , trigger_dag_id='dm_dap_monthly'
        , execution_date =  "{{ next_execution_date.in_timezone('Asia/Jakarta') }}'"  
        , wait_for_completion=True
        , reset_dag_run=True
        , retries = 2
    )

    Finish = PythonOperator(
            task_id='Finish',
            python_callable=function_master.finish,
            trigger_rule='all_done',
            provide_context=True,
            # execution_timeout=timeout_treshold,
            dag=AIRFLOW_VAR_DAP_DAG_ALIAS,
            retries = 2
        )
    
    Start >> t1 >> t2 >> t3 >> Finish 
    
        