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
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.cloud_storage_transfer_service import CloudDataTransferServiceRunJobOperator
from airflow.providers.google.cloud.sensors.cloud_storage_transfer_service import CloudDataTransferServiceJobStatusSensor

from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from DATA.utils.dap.load_config import ConfigFile
from DATA.operator.dap.function_sys_job_dap_omt_process_log_housekeeping_daily import Function

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
AIRFLOW_VAR_DAP_DAG_ID = "sys_job_dap_omt_process_log_housekeeping_daily"
AIRFLOW_VAR_DAP_DAG_DESCRIPTION = "System Job To Backup OMT Process Log to bucket Archive"
AIRFLOW_VAR_DAP_DAG_ALIAS = 'function_sys_job_dap_omt_process_log_housekeeping_daily'
AIRFLOW_VAR_DAP_DAG_PATH = '/DATA/config/dap/function_sys_job_dap_omt_process_log_housekeeping_daily/'
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

# Mendapatkan nilai dari AIRFLOW_VAR_DAP_OMT_PROCESS_LOG
omt_process_log = AIRFLOW_VAR_DAP_OMT_PROCESS_LOG  # Misalnya, "omt.omt_process_log"

# Memisahkan string berdasarkan titik
parts = omt_process_log.split('.')

# Mengambil bagian pertama dan kedua
omt_schema = parts[0]  # "omt"
omt_process_log_table = parts[1] if len(parts) > 1 else None  # "omt_process_log" jika ada, None jika tidak ada

with DAG(AIRFLOW_VAR_DAP_DAG_ID
    , default_args=default_args
    , description=AIRFLOW_VAR_DAP_DAG_DESCRIPTION
    , schedule_interval='01 00 * * *'
    , catchup=False
    , tags=["SYS","DAILY","OMT","HOUSEKEEPING","DAP"]) as AIRFLOW_VAR_DAP_DAG_ALIAS:

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
    
    # VALIDATE MASTER JOB
    validate_master_job = \
        BigQueryInsertJobOperator(
            task_id='validate_master_job'
            , configuration={
                "query": {
                    "query": "CALL `"+AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID+ "`." + AIRFLOW_VAR_DAP_DATASET_SP + "." + AIRFLOW_VAR_DAP_SP_VALIDATE_MASTER_JOB +"\
                                    ( \
                                    '"+ AIRFLOW_VAR_DAP_DAG_ID +"'\
                                    ,'"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID +"'\
                                    ,'"+ omt_schema +"'\
                                    ,'"+ omt_process_log_table +"'\
                                    , '" + AIRFLOW_VAR_DAP_CATEGORY_JOB + "'\
                                    )"
                    ,"useLegacySql": False,
                }
            }
            , location= AIRFLOW_VAR_DAP_LOCATION
            , project_id= AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID
            , retries= 5
            # execution_timeout=timeout_treshold
        )

    # Get Variable from OMT_MASTER_JOB
    get_variable_omt_master_job = PythonOperator(
        task_id='get_variable_omt_master_job'
        , python_callable=function_master.get_variable_omt_master_job
        , op_kwargs={'query_get_var_omt_master_job': " SELECT \
                                                            job_name ,	\
                                                            table_name ,	\
                                                            query_parameter ,	\
                                                            execution_date,	\
                                                            status_run_execution_date , \
                                                            category ,	\
                                                            enable_flag ,	\
                                                            prioritas,	\
                                                            source_gcs_folder , \
                                                            archive_gcs_folder ,\
                                                            retention_file_day ,\
                                                            retention_table_day ,\
                                                            query_create_table ,\
                                                            target_table_name ,\
                                                            table_sp_name ,\
                                                            sts_job_name \
                                                        FROM `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_OMT_MASTER_JOB + " \
                                                        WHERE 1=1\
                                                            AND JOB_NAME = '"+ AIRFLOW_VAR_DAP_DAG_ID +"'\
                                                            and TABLE_NAME = '" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + "' \
                                                        "
                                }
        , retries = 5
    )
        
    create_table_for_housekeeping = BigQueryOperator(
            task_id='create_table_for_housekeeping'
            , sql="SELECT * \
                 FROM `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + " \
                 WHERE 1=1 \
                    AND EXECUTION_DATE < DATE_SUB(CURRENT_DATE() , INTERVAL {{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job',key='retention_table_day_housekeeping') }} DAY) \
                    AND 0 <> {{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job',key='retention_table_day_housekeeping') }} \
                ORDER BY 1"
            , destination_dataset_table= AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_TEMP_CLEANSING + "."+ omt_schema+ "_" + omt_process_log_table + "_clean"  
            , write_disposition='WRITE_TRUNCATE'
            , create_disposition='CREATE_IF_NEEDED'
            , use_legacy_sql=False
            , retries = 10
        )

    housekeeping_data_to_archive = BashOperator(
        task_id='housekeeping_data_to_archive'
        , bash_command= "bq extract \
                        --location=" + AIRFLOW_VAR_DAP_LOCATION + "\
                        --destination_format=CSV  \
                        --field_delimiter=',' " + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + ":" + AIRFLOW_VAR_DAP_DATASET_TEMP_CLEANSING + "."+ omt_schema + "_" + omt_process_log_table + "_clean gs://"+ AIRFLOW_VAR_DAP_ARCHIVED_BUCKET +"/{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job',key='archive_gcs_folder_housekeeping') }}/data/"+ omt_schema +"_{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job',key='target_table_name_housekeeping') }}_clean_"+"{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}.csv"
        , retries = 10
    )

    delete_data_in_table_for_housekeeping = BigQueryInsertJobOperator(
            task_id='delete_data_in_table_for_housekeeping'
            ,configuration={
                            "query": {
                                "query": "DELETE \
                                            FROM `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + " \
                                            WHERE 1=1 \
                                                AND EXECUTION_DATE < DATE_SUB(CURRENT_DATE() , INTERVAL {{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job',key='retention_table_day_housekeeping') }} DAY) \
                                                AND 0 <> {{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job',key='retention_table_day_housekeeping') }} \
                                            ",
                                "useLegacySql": False,
                                }
                            }
            ,location=AIRFLOW_VAR_DAP_LOCATION
            ,project_id=AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID
            ,execution_timeout=timeout_treshold
            , retries = 10
        )

    Start >> validate_master_job >> get_variable_omt_master_job >> create_table_for_housekeeping >> housekeeping_data_to_archive >> delete_data_in_table_for_housekeeping >> Finish