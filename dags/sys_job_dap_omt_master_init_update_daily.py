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
from DATA.operator.dap.function_sys_job_dap_omt_master_init_update_daily import Function

import re
import os
import pendulum
import pytz
from airflow.utils.dates import days_ago

local_tz = pendulum.timezone("Asia/Jakarta")
start_date_utc = days_ago(1)
start_date_local = start_date_utc.astimezone(tz=local_tz)

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
AIRFLOW_VAR_DAP_DATASET_DM_STG = Variable.get('AIRFLOW_VAR_DAP_DATASET_DM_STG')
AIRFLOW_VAR_DAP_DATASET_DM_MASKED = Variable.get('AIRFLOW_VAR_DAP_DATASET_DM_MASKED')
AIRFLOW_VAR_DAP_DATASET_DW = Variable.get('AIRFLOW_VAR_DAP_DATASET_DW')
AIRFLOW_VAR_DAP_DATASET_MISPLUS = Variable.get('AIRFLOW_VAR_DAP_DATASET_MISPLUS')
AIRFLOW_VAR_DAP_DATASET_SP = Variable.get('AIRFLOW_VAR_DAP_DATASET_SP')
AIRFLOW_VAR_DAP_DATASET_TEMP = Variable.get('AIRFLOW_VAR_DAP_DATASET_TEMP')
AIRFLOW_VAR_DAP_DATASET_TEMP_CLEANSING = Variable.get('AIRFLOW_VAR_DAP_DATASET_TEMP_CLEANSING')
AIRFLOW_VAR_DAP_DATASET_TEMP_SP	= Variable.get('AIRFLOW_VAR_DAP_DATASET_TEMP_SP')
AIRFLOW_VAR_DAP_DATASET_UDF	= Variable.get('AIRFLOW_VAR_DAP_DATASET_UDF')
AIRFLOW_VAR_DAP_SP_VALIDATE_MASTER_JOB = Variable.get('AIRFLOW_VAR_DAP_SP_VALIDATE_MASTER_JOB')

AIRFLOW_VAR_DAP_JOB_TYPE = 'system'
AIRFLOW_VAR_DAP_DAG_ID = "sys_job_dap_omt_master_init_update_daily"
AIRFLOW_VAR_DAP_DAG_DESCRIPTION = "System Job To Update OMT Master Job to Initial status 0 and current execution date"
AIRFLOW_VAR_DAP_DAG_ALIAS = 'sys_job_dap_omt_master_init_update_daily'
AIRFLOW_VAR_DAP_DAG_PATH = '/DATA/config/dap/sys_job_dap_omt_master_init_update_daily/'
AIRFLOW_VAR_DAP_CATEGORY_JOB = 'SYS'

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
    'start_date': start_date_utc.astimezone(tz=local_tz)
    , 'retries': 0
}

with DAG(AIRFLOW_VAR_DAP_DAG_ID
    , default_args=default_args
    , description=AIRFLOW_VAR_DAP_DAG_DESCRIPTION
    , schedule_interval='01 00 * * *'
    , catchup=False
    , tags=["SYS","DAILY","OMT","DAP"]) as AIRFLOW_VAR_DAP_DAG_ALIAS:

    Start = EmptyOperator(task_id="Start")
    
    Finish = PythonOperator(
            task_id='Finish',
            python_callable=function_master.finish,
            trigger_rule='all_done',
            provide_context=True,
            # execution_timeout=timeout_treshold,
            dag=AIRFLOW_VAR_DAP_DAG_ALIAS,
            retries = 5
        )

        
    update_init_omt_master_job = BigQueryInsertJobOperator(
            task_id='update_init_omt_master_job'
            ,configuration={
                            "query": {
                                "query": "UPDATE `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + '.' + AIRFLOW_VAR_DAP_OMT_MASTER_JOB + "` \
                                        SET status_run_execution_date= '0'  \
                                            , execution_date = CURRENT_DATE('Asia/Jakarta') \
                                        WHERE 1=1 \
                                    "
                                , "useLegacySql": False,
                                }
                            }
            ,location=AIRFLOW_VAR_DAP_LOCATION
            ,project_id=AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID
            ,execution_timeout=timeout_treshold
            , retries = 10
        )

    Start >> update_init_omt_master_job >> Finish