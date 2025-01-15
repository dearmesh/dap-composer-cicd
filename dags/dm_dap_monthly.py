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
from DATA.operator.dap.function_dm_dap_monthly import Function

import re
import os
import pendulum
import pytz

local_tz = pendulum.timezone("Asia/Jakarta")

_BASE_FOLDER = settings.DAGS_FOLDER
timeout_treshold = timedelta(minutes=5)


#Initial Variable
AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID = Variable.get('AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID')
AIRFLOW_VAR_DAP_SANDBOX_PROJECT_ID = Variable.get('AIRFLOW_VAR_DAP_SANDBOX_PROJECT_ID')
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
AIRFLOW_VAR_DAP_SP_CHECK_DEPENDENCY = Variable.get('AIRFLOW_VAR_DAP_SP_CHECK_DEPENDENCY')
AIRFLOW_VAR_DAP_SP_VALIDATE_MASTER_JOB = Variable.get('AIRFLOW_VAR_DAP_SP_VALIDATE_MASTER_JOB')

AIRFLOW_VAR_DAP_JOB_TYPE = 'dm_dap_monthly'
AIRFLOW_VAR_DAP_DAG_ID = "dm_dap_monthly"
AIRFLOW_VAR_DAP_DAG_DESCRIPTION = "Job for DM DAP Monthly"
AIRFLOW_VAR_DAP_DAG_ALIAS = 'dm_dap_monthly'
AIRFLOW_VAR_DAP_DAG_PATH = '/DATA/config/dap/dm_dap_monthly/'
AIRFLOW_VAR_DAP_CATEGORY_JOB = 'DM'

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

with DAG(AIRFLOW_VAR_DAP_DAG_ID
    , default_args=default_args
    , description=AIRFLOW_VAR_DAP_DAG_DESCRIPTION
    , schedule_interval=None
    , catchup=False
    , tags=["DM_STG","MONTHLY","DAP"]) as AIRFLOW_VAR_DAP_DAG_ALIAS:

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

    for config_name, xfer_config in yaml_configs:

        AIRFLOW_VAR_DAP_JOB_ID = function_master.get_current_time() #'{{ (data_interval_start + macros.timedelta(hours=7)).strftime("%Y%m%d%H%M%S") }}'

        # VALIDATE MASTER JOB
        validate_master_job = \
            BigQueryInsertJobOperator(
                task_id='validate_master_job_{}'.format(config_name.replace("_yaml","")),
                configuration={
                    "query": {
                        "query": "CALL `"+AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID+ "`." + AIRFLOW_VAR_DAP_DATASET_SP + "." + AIRFLOW_VAR_DAP_SP_VALIDATE_MASTER_JOB +"\
                                        ( \
                                        '"+AIRFLOW_VAR_DAP_DAG_ID+"'\
                                        ,'"+AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID+"'\
                                        ,'"+AIRFLOW_VAR_DAP_DATASET_DM+"'\
                                        ,'"+ xfer_config.main_bigquery_table.table_name +"'\
                                        , '" + AIRFLOW_VAR_DAP_CATEGORY_JOB + "'\
                                        )"
                        ,"useLegacySql": False,
                    }
                },
                location= AIRFLOW_VAR_DAP_LOCATION,
                project_id= AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID
                , retries= 5
                # execution_timeout=timeout_treshold
            )

        # Get Variable from OMT_MASTER_JOB
        get_variable_omt_master_job = PythonOperator(
            task_id='get_variable_omt_master_job_{}'.format(config_name.replace("_yaml", ""))
            , python_callable=function_master.get_variable_omt_master_job
            , op_kwargs={'query_get_var_{}'.format(config_name.replace("_yaml", "")): " SELECT \
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
                                                                                            and TABLE_NAME = '" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + "." + xfer_config.main_bigquery_table.table_name + "' \
                                                                                        "
                                    }
            , retries = 5
        )

        

        # VALIDATE DUPLICATE RUN
        validate_enable_flag_and_duplicate_run = PythonOperator(
            task_id='validate_enable_flag_and_duplicate_run_{}'.format(config_name.replace("_yaml", ""))
            , python_callable=function_master.validate_enable_flag_and_duplicate_run
            , op_kwargs={'query_validate_{}'.format(config_name.replace("_yaml", "")): " SELECT \
                                                                                        ( SELECT case when count(1) >= 1 then 1 else 0 end as COUNT_MAIN_PROCESS \
                                                                                            FROM `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_OMT_MASTER_JOB + " \
                                                                                            WHERE 1=1  \
                                                                                                AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                                AND TABLE_NAME = '" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                                                                AND STATUS_RUN_EXECUTION_DATE = '1' \
                                                                                        ) COUNT_MAIN_PROCESS \
                                                                                        ,( \
                                                                                            SELECT \
                                                                                                    CASE \
                                                                                                    WHEN COUNT(1) >= 1 THEN 1 \
                                                                                                    ELSE 0 \
                                                                                                END \
                                                                                                    AS CHECK_ENABLE_FLAG \
                                                                                                FROM \
                                                                                                    `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_OMT_MASTER_JOB + " \
                                                                                                WHERE \
                                                                                                    1=1 \
                                                                                                    AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                                    AND TABLE_NAME = '" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                                                                    AND ENABLE_FLAG = '1' \
                                                                                            ) CHECK_ENABLE_FLAG \
                                                                                        "
                                    }
            , retries = 5
        )

        # branch1
        branch_validate = BranchPythonOperator(
            task_id='branch_validate_{}'.format(config_name.replace("_yaml","")) 
            , dag=AIRFLOW_VAR_DAP_DAG_ALIAS
            , python_callable= function_master.branch_validate_enable_flag_and_duplicate_run
            , provide_context=True
            # , execution_timeout=timeout_treshold
            , trigger_rule='none_skipped'
            ,retries=2
        ) 
        
        # CREATE TABLE TARGET
        create_table_target = \
            BigQueryInsertJobOperator(
                task_id='create_table_target_{}'.format(config_name.replace("_yaml","")),
                configuration={
                    "query": {
                        "query": xfer_config.main_bigquery_table.schema_fields.replace('v_dataset_id',AIRFLOW_VAR_DAP_DATASET_DM),
                        "useLegacySql": False,
                    }
                },
                location= AIRFLOW_VAR_DAP_LOCATION,
                project_id= AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID
                , retries= 5
                # execution_timeout=timeout_treshold
            )
        
        # CREATE TABLE TARGET
        create_table_sandbox = \
            BigQueryInsertJobOperator(
                task_id='create_table_sandbox_{}'.format(config_name.replace("_yaml",""))
                , configuration={
                    "query": {
                        "query": xfer_config.main_bigquery_table.schema_fields.replace('v_dataset_id',AIRFLOW_VAR_DAP_DATASET_DM)
                        , "useLegacySql": False
                    }
                },
                location= AIRFLOW_VAR_DAP_LOCATION
                , project_id= AIRFLOW_VAR_DAP_SANDBOX_PROJECT_ID
                , retries= 5
                # execution_timeout=timeout_treshold
            )
        
        # Make Sure No Running Or Error that mix with Success / Done Data in Prc dt <= Curent EXECUTION_DATE
        update_if_error_exists = BigQueryInsertJobOperator(
                task_id='update_if_error_exists_{}'.format(config_name.replace("_yaml", ""))
               ,configuration={
                                "query": {
                                    "query": "UPDATE `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + " \
                                              SET STATUS = 'ERROR' \
                                                WHERE 1=1  \
                                                    AND JOB_TYPE = '" + AIRFLOW_VAR_DAP_JOB_TYPE + "' \
                                                    AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                    AND TRG_TBL_NAME = '" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                    AND EXECUTION_DATE IN (SELECT DISTINCT EXECUTION_DATE \
                                                                    FROM `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + " \
                                                                    WHERE 1=1 \
                                                                        AND JOB_TYPE = '" + AIRFLOW_VAR_DAP_JOB_TYPE + "' \
                                                                        AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                        AND TRG_TBL_NAME = '" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                                        AND STATUS IN ('ERROR','RUNNING')  \
                                                                        AND EXECUTION_DATE <= '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' \
                                                                        AND EXECUTION_DATE BETWEEN DATE_SUB(CAST('{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' AS DATE) , INTERVAL " + AIRFLOW_VAR_DAP_OMT_CHECK_DT_IN_MONTH + " MONTH) \
                                                                                           AND CAST('{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' AS DATE) \
                                                                    ) " ,
                                    "useLegacySql": False,
                                  }
                              }
               ,location=AIRFLOW_VAR_DAP_LOCATION
               ,project_id=AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID
               ,execution_timeout=timeout_treshold
               ,on_failure_callback=partial(function_master.error_handling, AIRFLOW_VAR_DAP_DAG_ID, "DATAMART_QUERY", AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + "." + config_name.replace("_yaml", ""))
               , retries = 10
            )
        
        # DELETE OMT PROCESS LOG dengan status ERROR
        delete_omt_error = PythonOperator(
            task_id='delete_omt_error_{}'.format(config_name.replace("_yaml", ""))
           ,python_callable=function_master.delete_omt
           ,on_failure_callback=partial(function_master.error_handling, AIRFLOW_VAR_DAP_DAG_ID, "DATAMART_QUERY", AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + "." + config_name.replace("_yaml", ""))
           ,op_kwargs={'query_{}'.format(config_name.replace("_yaml", "")): " DELETE `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + "`  \
                                                                              WHERE 1=1 \
                                                                                AND SRC_FILE_OR_TBL_NAME ='DATAMART_QUERY' \
                                                                                AND TRG_TBL_NAME ='" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                                                AND JOB_TYPE = '" + AIRFLOW_VAR_DAP_JOB_TYPE + "' \
                                                                                AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                AND STATUS in ('ERROR') \
                                                                              ;"
                      , 'query2_{}'.format(config_name.replace("_yaml", "")): " DELETE `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + "`  \
                                                                              WHERE 1=1 \
                                                                                AND SRC_FILE_OR_TBL_NAME ='DATAMART_QUERY' \
                                                                                AND TRG_TBL_NAME ='" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                                                AND JOB_TYPE = '" + AIRFLOW_VAR_DAP_JOB_TYPE + "' \
                                                                                AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                AND EXECUTION_DATE = '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' \
                                                                              ;"
                       }
            , retries = 10
        )

        # INSERT OMT PROCESS LOG dengan status Running untuk FLAG JOB STATUS
        insert_omt_job_running = PythonOperator(
            task_id='insert_omt_job_running_{}'.format(config_name.replace("_yaml", ""))
           ,python_callable=function_master.insert_omt_job_running
           ,on_failure_callback=partial(function_master.error_handling, AIRFLOW_VAR_DAP_DAG_ID, "DATAMART_QUERY", AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + "." + config_name.replace("_yaml", ""))
           ,op_kwargs={'query_{}'.format(config_name.replace("_yaml", "")): " INSERT INTO `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + "  \
                                                                                  VALUES ('" + AIRFLOW_VAR_DAP_JOB_TYPE + "'  \
                                                                                          , '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                          , CURRENT_DATETIME('Asia/Jakarta') \
                                                                                          , NULL  \
                                                                                          , NULL   \
                                                                                          , 'DATAMART_QUERY'  \
                                                                                          , 'N/A'  \
                                                                                          , '" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}'   \
                                                                                          , 0   \
                                                                                          , 0  \
                                                                                          , 0   \
                                                                                          , 'RUNNING'   \
                                                                                          , 'JOB_STATUS_FLAG' \
                                                                                          , '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}'  \
                                                                                          , '" + AIRFLOW_VAR_DAP_JOB_ID + "' \
                                                                                          , CASE \
                                                                                                WHEN LAST_DAY(CAST(DATE_SUB('{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' , INTERVAL 1 MONTH) as DATE)) < CAST(CAST('"+ AIRFLOW_VAR_DAP_MIN_DATE_TRANSACTION_INITIAL +"' AS DATETIME) AS DATE) THEN CAST(LAST_DAY(CAST('"+ AIRFLOW_VAR_DAP_MIN_DATE_TRANSACTION_INITIAL +"' AS DATETIME)) AS DATE)\
                                                                                                ELSE LAST_DAY(CAST(DATE_SUB('{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' , INTERVAL 1 MONTH) as DATE))\
                                                                                            END  \
                                                                                          ); "
                        }
            , retries = 10
        )

        #PROCESS AUTOFILL_BACKLOG
        autofill_backlog = PythonOperator(
                task_id='autofill_backlog_{}'.format(config_name.replace("_yaml",""))
                , python_callable=function_master.autofill_backlog
                , on_failure_callback=partial(function_master.error_handling, AIRFLOW_VAR_DAP_DAG_ID, "DATAMART_QUERY", AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + "." + config_name.replace("_yaml", ""))
                , retries = AIRFLOW_VAR_DAP_RETRIES
                , retry_delay= AIRFLOW_VAR_DAP_RETRY_DELAY
                , op_kwargs={'query_backlog_data_with_process_{}'.format(config_name.replace("_yaml","")) : "SELECT MAX(SUBSTR(CAST(day_date AS STRING),1,10)) missing_execution_date \
	                                                                                                , LAST_DAY(DATE_SUB(CAST(SUBSTR(CAST(day_date AS STRING),1,10) AS DATE), INTERVAL 1 MONTH)) last_day_business_date \
                                                                                                FROM `"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID +"`."+ AIRFLOW_VAR_DAP_OMT_CALENDAR_DATE +" \
                                                                                                WHERE 1=1 \
                                                                                                    and day_date < DATE_TRUNC('{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' , MONTH) \
                                                                                                    and day_date >= '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' - " + AIRFLOW_VAR_DAP_OMT_AUTOFILL_BACKLOG_RETENTION + "   \
                                                                                                    and day_date >= PARSE_DATETIME('%Y-%m-%d %H:%M:%E3S' , '" + AIRFLOW_VAR_DAP_MIN_DATE_TRANSACTION_INITIAL + "')  \
                                                                                                    and CAST(day_date as DATE) not in ( \
                                                                                                                                    SELECT EXECUTION_DATE \
                                                                                                                                    FROM `"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID +"`."+ AIRFLOW_VAR_DAP_OMT_PROCESS_LOG +" a \
                                                                                                                                    WHERE 1=1 \
                                                                                                                                        AND TRG_TBL_NAME ='"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                                                                                                        AND JOB_TYPE = '" + AIRFLOW_VAR_DAP_JOB_TYPE + "' \
                                                                                                                                        AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                                                                        AND SRC_FILE_OR_TBL_NAME ='DATAMART_QUERY' \
                                                                                                                                        AND STATUS IN ('DONE')  \
                                                                                                                                        AND IFNULL(STATUS_DESCRIPTION,'') NOT IN ('JOB_STATUS_FLAG') \
                                                                                                                                        and EXECUTION_DATE BETWEEN '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' - " + AIRFLOW_VAR_DAP_OMT_AUTOFILL_BACKLOG_RETENTION +" and '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' \
                                                                                                                                ) \
                                                                                                GROUP by 2 \
                                                                                                ORDER by 1"    
                            , 'update_if_error_exists_{}'.format(config_name.replace("_yaml","")) : "UPDATE `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + " \
                                                                                                    SET STATUS = 'ERROR' \
                                                                                                        WHERE 1=1  \
                                                                                                            AND JOB_TYPE = '" + AIRFLOW_VAR_DAP_JOB_TYPE + "' \
                                                                                                            AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                                            AND TRG_TBL_NAME = '" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                                                                            AND SRC_FILE_OR_TBL_NAME ='DATAMART_QUERY' \
                                                                                                            AND EXECUTION_DATE = '{execution_date}' \
                                                                                                            AND EXECUTION_DATE IN (SELECT EXECUTION_DATE \
                                                                                                                                    FROM `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + " \
                                                                                                                                    WHERE 1=1 \
                                                                                                                                        AND JOB_TYPE = '" + AIRFLOW_VAR_DAP_JOB_TYPE + "' \
                                                                                                                                        AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                                                                        AND TRG_TBL_NAME = '" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                                                                                                        AND SRC_FILE_OR_TBL_NAME ='DATAMART_QUERY' \
                                                                                                                                        AND STATUS IN ('ERROR','RUNNING')  \
                                                                                                                                        AND EXECUTION_DATE = '{execution_date}' \
                                                                                                                                    )  \
                                                                                                ;"
                            , 'delete_omt_error_{}'.format(config_name.replace("_yaml","")) : " DELETE `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + "  \
                                                                                                WHERE 1=1 \
                                                                                                    AND JOB_TYPE = '" + AIRFLOW_VAR_DAP_JOB_TYPE + "' \
                                                                                                    AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                                    AND TRG_TBL_NAME = '" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                                                                    AND SRC_FILE_OR_TBL_NAME ='DATAMART_QUERY' \
                                                                                                    AND STATUS IN ('ERROR')  \
                                                                                                    AND EXECUTION_DATE = '{execution_date}' \
                                                                                                ;"                                                                       
                            , 'check_dependency_backlog_{}'.format(config_name.replace("_yaml","")) : "call `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_DATASET_SP + "." + AIRFLOW_VAR_DAP_SP_CHECK_DEPENDENCY + 
                                                                                                        " ( '" + AIRFLOW_VAR_DAP_JOB_TYPE + "'  \
                                                                                                            , '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                                            , '" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "' \
                                                                                                            ,'" + AIRFLOW_VAR_DAP_DATASET_DM + "' \
                                                                                                            ,'{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                                                                            , '{}' \
                                                                                                            , 'MONTHLY' \
                                                                                                            , '" + AIRFLOW_VAR_DAP_OMT_JOB_DEPENDENCY + "'\
                                                                                                            , '" + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + "'\
                                                                                                            , '" + AIRFLOW_VAR_DAP_DATASET_TEMP_SP + "'\
                                                                                                          ) "
                            , 'insert_omt_backlog_with_process_{}'.format(config_name.replace("_yaml","")) : "INSERT INTO `"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID +"`."+ AIRFLOW_VAR_DAP_OMT_PROCESS_LOG +"  \
                                                                                                                SELECT SUB.JOB_TYPE  \
                                                                                                                    , SUB.JOB_NAME \
                                                                                                                    , SUB.START_DATE \
                                                                                                                    , SUB.END_DATE  \
                                                                                                                    , CAST(CAST(SUB.execution_date AS STRING FORMAT 'YYYY-MM-DD') || ' 00:00:01' AS DATETIME) MAX_LAST_UPDATE_DATE \
                                                                                                                    , SUB.SRC_FILE_OR_TBL_NAME \
                                                                                                                    , SUB.TRG_TEMP_FILE \
                                                                                                                    , SUB.TRG_TBL_NAME \
                                                                                                                    , SUB.DELTA_DATA_COUNT \
                                                                                                                    , SUB.SRC_DATA_COUNT \
                                                                                                                    , SUB.TRG_DATA_COUNT \
                                                                                                                    , SUB.STATUS \
                                                                                                                    , SUB.STATUS_DESCRIPTION \
                                                                                                                    , SUB.execution_date \
                                                                                                                    , SUB.JOB_ID \
                                                                                                                    , SUB.BUSINESS_DATE \
                                                                                                                FROM  \
                                                                                                                ( \
                                                                                                                    SELECT \
                                                                                                                            '" + AIRFLOW_VAR_DAP_JOB_TYPE + "'  JOB_TYPE\
                                                                                                                            , '"+ AIRFLOW_VAR_DAP_DAG_ID +"' JOB_NAME\
                                                                                                                            , CURRENT_DATETIME('Asia/Jakarta') START_DATE\
                                                                                                                            , CAST(NULL as DATETIME)  END_DATE\
                                                                                                                            , CAST(NULL as DATETIME)   MAX_LAST_UPDATE_DATE\
                                                                                                                            , 'DATAMART_QUERY' SRC_FILE_OR_TBL_NAME \
                                                                                                                            , 'N/A'  TRG_TEMP_FILE\
                                                                                                                            , '"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM +".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}'  TRG_TBL_NAME \
                                                                                                                            , 0  DELTA_DATA_COUNT \
                                                                                                                            , 0   SRC_DATA_COUNT\
                                                                                                                            , NULL   TRG_DATA_COUNT\
                                                                                                                            , 'RUNNING'  STATUS  \
                                                                                                                            , 'AUTOFILL_BACKLOG_WITH_PROCESS' STATUS_DESCRIPTION \
                                                                                                                            , CAST('{execution_date}' AS DATE)  execution_date \
                                                                                                                            , '"+ AIRFLOW_VAR_DAP_JOB_ID +"'  JOB_ID \
                                                                                                                            , CASE \
                                                                                                                                WHEN LAST_DAY(DATE_SUB(CAST('{execution_date}' AS DATE), INTERVAL 1 MONTH)) < CAST(CAST('"+ AIRFLOW_VAR_DAP_MIN_DATE_TRANSACTION_INITIAL +"' AS DATETIME) AS DATE) THEN CAST(LAST_DAY(CAST('"+ AIRFLOW_VAR_DAP_MIN_DATE_TRANSACTION_INITIAL +"' AS DATETIME)) AS DATE)\
                                                                                                                                ELSE LAST_DAY(DATE_SUB(CAST('{execution_date}' AS DATE), INTERVAL 1 MONTH))\
                                                                                                                            END BUSINESS_DATE\
                                                                                                                    FROM (select 1 limit 1) \
                                                                                                                    WHERE 1=1 \
                                                                                                                ) SUB  \
                                                                                                                ;"
                            , 'query_insert_backlog_{}'.format(config_name.replace("_yaml","")) : "CALL " + AIRFLOW_VAR_DAP_DATASET_SP + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='table_sp_name_" + config_name.replace("_yaml", "") + "') }} " +
                                                                                                    " ( '" + AIRFLOW_VAR_DAP_JOB_TYPE + "'  \
                                                                                                        , '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                                        , '" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "' \
                                                                                                        , '" + AIRFLOW_VAR_DAP_DATASET_DM + "' \
                                                                                                        , '{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}'  \
                                                                                                        , '{}' \
                                                                                                        , '" + AIRFLOW_VAR_DAP_JOB_ID + "' \
                                                                                                        , '" + AIRFLOW_VAR_DAP_OMT_JOB_DEPENDENCY + "'\
                                                                                                        , '" + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + "'\
                                                                                                        , '" + AIRFLOW_VAR_DAP_DATASET_TEMP_SP + "'\
                                                                                                    ) "    
                            , 'insert_omt_backlog_insert_only_{}'.format(config_name.replace("_yaml","")) : "INSERT INTO `"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID +"`."+ AIRFLOW_VAR_DAP_OMT_PROCESS_LOG +"  \
                                                                                                            SELECT opl.JOB_TYPE  \
                                                                                                                , opl.JOB_NAME \
                                                                                                                , opl.START_DATE \
                                                                                                                , opl.END_DATE  \
                                                                                                                , CAST(list_tanggal.missing_execution_date|| ' 00:00:01' AS DATETIME) MAX_LAST_UPDATE_DATE \
                                                                                                                , opl.SRC_FILE_OR_TBL_NAME \
                                                                                                                , opl.TRG_TEMP_FILE \
                                                                                                                , opl.TRG_TBL_NAME \
                                                                                                                , opl.DELTA_DATA_COUNT \
                                                                                                                , opl.SRC_DATA_COUNT \
                                                                                                                , opl.TRG_DATA_COUNT \
                                                                                                                , 'DONE' \
                                                                                                                , 'AUTOFILL_BACKLOG' \
                                                                                                                , CAST(list_tanggal.missing_execution_date AS DATE) \
                                                                                                                , opl.JOB_ID  \
                                                                                                                , opl.BUSINESS_DATE \
                                                                                                            FROM OMT.OMT_PROCESS_LOG opl\
                                                                                                            , (SELECT SUBSTR(CAST(day_date AS STRING),1,10) missing_execution_date \
                                                                                                                    , '{execution_date}' execution_date_reference\
                                                                                                                FROM `"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID +"`."+ AIRFLOW_VAR_DAP_OMT_CALENDAR_DATE +" \
                                                                                                                WHERE 1=1 \
                                                                                                                    and day_date < '{execution_date}' \
                                                                                                                    and day_date >= DATE_TRUNC('{execution_date}' , MONTH) \
                                                                                                                    and day_date >= PARSE_DATETIME('%Y-%m-%d %H:%M:%E3S' , '" + AIRFLOW_VAR_DAP_MIN_DATE_TRANSACTION_INITIAL + "')  \
                                                                                                                    and CAST(day_date as DATE) not in ( \
                                                                                                                                                        SELECT EXECUTION_DATE \
                                                                                                                                                        FROM `"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID +"`."+ AIRFLOW_VAR_DAP_OMT_PROCESS_LOG +" a \
                                                                                                                                                        WHERE 1=1 \
                                                                                                                                                            AND TRG_TBL_NAME ='"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                                                                                                                            AND JOB_TYPE = '" + AIRFLOW_VAR_DAP_JOB_TYPE + "' \
                                                                                                                                                            AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                                                                                            AND SRC_FILE_OR_TBL_NAME ='DATAMART_QUERY' \
                                                                                                                                                            AND STATUS IN ('DONE')  \
                                                                                                                                                            AND IFNULL(STATUS_DESCRIPTION,'') NOT IN ('JOB_STATUS_FLAG') \
                                                                                                                                                            and EXECUTION_DATE BETWEEN '{execution_date}' - " + AIRFLOW_VAR_DAP_OMT_AUTOFILL_BACKLOG_RETENTION +" and '{execution_date}' \
                                                                                                                                                        )\
                                                                                                                ) list_tanggal\
                                                                                                            WHERE 1=1 \
                                                                                                                AND EXECUTION_DATE = CAST(list_tanggal.execution_date_reference AS DATE) \
                                                                                                                AND JOB_TYPE = '" + AIRFLOW_VAR_DAP_JOB_TYPE + "' \
                                                                                                                AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                                                AND TRG_TBL_NAME = '"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM +".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                                                                                AND SRC_FILE_OR_TBL_NAME = 'DATAMART_QUERY' \
                                                                                                                AND IFNULL(STATUS_DESCRIPTION,'') <> 'JOB_STATUS_FLAG' \
                                                                                                                AND BUSINESS_DATE >= DATE_SUB(CAST(list_tanggal.execution_date_reference AS DATE), INTERVAL 1 MONTH) \
                                                                                                            ;" 
                            , 'delete_sandbox_data_{}'.format(config_name.replace("_yaml","")) : "DELETE \
                                                                                                FROM `" + AIRFLOW_VAR_DAP_SANDBOX_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }} \
                                                                                                WHERE 1=1 \
                                                                                                    AND BUSINESS_DATE = CASE \
                                                                                                                                WHEN LAST_DAY(DATE_SUB(CAST('{execution_date}' AS DATE), INTERVAL 1 MONTH)) < CAST(CAST('"+ AIRFLOW_VAR_DAP_MIN_DATE_TRANSACTION_INITIAL +"' AS DATETIME) AS DATE) THEN CAST(LAST_DAY(CAST('"+ AIRFLOW_VAR_DAP_MIN_DATE_TRANSACTION_INITIAL +"' AS DATETIME)) AS DATE)\
                                                                                                                                ELSE LAST_DAY(DATE_SUB(CAST('{execution_date}' AS DATE), INTERVAL 1 MONTH))\
                                                                                                                            END\
                                                                                                    "
                            , 'send_to_sandbox_{}'.format(config_name.replace("_yaml","")) : " INSERT INTO `" + AIRFLOW_VAR_DAP_SANDBOX_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }} \
                                                                                                SELECT * \
                                                                                                FROM `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }} \
                                                                                                WHERE 1=1 \
                                                                                                    AND BUSINESS_DATE = CASE \
                                                                                                                                WHEN LAST_DAY(DATE_SUB(CAST('{execution_date}' AS DATE), INTERVAL 1 MONTH)) < CAST(CAST('"+ AIRFLOW_VAR_DAP_MIN_DATE_TRANSACTION_INITIAL +"' AS DATETIME) AS DATE) THEN CAST(LAST_DAY(CAST('"+ AIRFLOW_VAR_DAP_MIN_DATE_TRANSACTION_INITIAL +"' AS DATETIME)) AS DATE)\
                                                                                                                                ELSE LAST_DAY(DATE_SUB(CAST('{execution_date}' AS DATE), INTERVAL 1 MONTH))\
                                                                                                                        END \
                                                                                             "
                            }        
            )
        
        check_dependency_job_running = \
            BigQueryInsertJobOperator(
                task_id='check_dependency_job_running_{}'.format(config_name.replace("_yaml",""))
                , configuration={
                    "query": {
                                "query":"call `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_DATASET_SP + "." + AIRFLOW_VAR_DAP_SP_CHECK_DEPENDENCY + "_status_running" + 
                                                " ( '" + AIRFLOW_VAR_DAP_JOB_TYPE + "'  \
                                                    , '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                    , '" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "' \
                                                    ,'" + AIRFLOW_VAR_DAP_DATASET_DM + "' \
                                                    ,'{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                    , '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' \
                                                    , '" + AIRFLOW_VAR_DAP_OMT_JOB_DEPENDENCY + "'\
                                                    , '" + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + "'\
                                                )"
                                , "useLegacySql": False
                            }
                    }
                , location= AIRFLOW_VAR_DAP_LOCATION
                , project_id= AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID
                , retries = AIRFLOW_VAR_DAP_RETRIES
                , retry_delay= AIRFLOW_VAR_DAP_RETRY_DELAY
            )

        check_dependency_datamart = \
            BigQueryInsertJobOperator(
                task_id='check_dependency_datamart_{}'.format(config_name.replace("_yaml",""))
                , configuration={
                    "query": {
                                "query":"call `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_DATASET_SP + "." + AIRFLOW_VAR_DAP_SP_CHECK_DEPENDENCY + 
                                                " ( '" + AIRFLOW_VAR_DAP_JOB_TYPE + "'  \
                                                    , '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                    , '" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "' \
                                                    ,'" + AIRFLOW_VAR_DAP_DATASET_DM + "' \
                                                    ,'{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                    , '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' \
                                                    , 'MONTHLY' \
                                                    , '" + AIRFLOW_VAR_DAP_OMT_JOB_DEPENDENCY + "'\
                                                    , '" + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + "'\
                                                    , '" + AIRFLOW_VAR_DAP_DATASET_TEMP_SP + "'\
                                                    )"
                                , "useLegacySql": False
                            }
                    }
                , location= AIRFLOW_VAR_DAP_LOCATION
                , project_id= AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID
                , retries = AIRFLOW_VAR_DAP_RETRIES
                , retry_delay= AIRFLOW_VAR_DAP_RETRY_DELAY
            )
        
        # INSERT OMT PROCESS LOG dengan status Running
        insert_omt_running = PythonOperator(
                task_id='insert_omt_running_{}'.format(config_name.replace("_yaml",""))
                , python_callable=function_master.insert_omt
                , op_kwargs={'query_{}'.format(config_name.replace("_yaml","")) : "INSERT INTO `"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID +"`."+ AIRFLOW_VAR_DAP_OMT_PROCESS_LOG +"  \
                                                                                    SELECT SUB.JOB_TYPE  \
                                                                                        , SUB.JOB_NAME \
                                                                                        , SUB.START_DATE \
                                                                                        , SUB.END_DATE  \
                                                                                        , CAST(CAST(SUB.execution_date AS STRING FORMAT 'YYYY-MM-DD') || ' 00:00:01' AS DATETIME) MAX_LAST_UPDATE_DATE \
                                                                                        , SUB.SRC_FILE_OR_TBL_NAME \
                                                                                        , SUB.TRG_TEMP_FILE \
                                                                                        , SUB.TRG_TBL_NAME \
                                                                                        , SUB.DELTA_DATA_COUNT \
                                                                                        , SUB.SRC_DATA_COUNT \
                                                                                        , SUB.TRG_DATA_COUNT \
                                                                                        , SUB.STATUS \
                                                                                        , SUB.STATUS_DESCRIPTION \
                                                                                        , SUB.execution_date \
                                                                                        , SUB.JOB_ID \
                                                                                        , SUB.BUSINESS_DATE \
                                                                                    FROM  \
                                                                                    ( \
                                                                                        SELECT \
                                                                                                '" + AIRFLOW_VAR_DAP_JOB_TYPE + "'  JOB_TYPE\
                                                                                                , '"+ AIRFLOW_VAR_DAP_DAG_ID +"' JOB_NAME\
                                                                                                , CURRENT_DATETIME('Asia/Jakarta') START_DATE\
                                                                                                , CAST(NULL as DATETIME)  END_DATE\
                                                                                                , CAST(NULL as DATETIME)   MAX_LAST_UPDATE_DATE\
                                                                                                , 'DATAMART_QUERY' SRC_FILE_OR_TBL_NAME \
                                                                                                , 'N/A'  TRG_TEMP_FILE\
                                                                                                , '"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM +".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}'  TRG_TBL_NAME \
                                                                                                , 0  DELTA_DATA_COUNT \
                                                                                                , 0   SRC_DATA_COUNT\
                                                                                                , NULL   TRG_DATA_COUNT\
                                                                                                , 'RUNNING'  STATUS  \
                                                                                                , CAST(NULL AS STRING) STATUS_DESCRIPTION \
                                                                                                , CAST('{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' AS DATE)  execution_date \
                                                                                                , '"+ AIRFLOW_VAR_DAP_JOB_ID +"'  JOB_ID \
                                                                                                , CASE \
                                                                                                    WHEN LAST_DAY(CAST(DATE_SUB('{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' , INTERVAL 1 MONTH) as DATE)) < CAST(CAST('"+ AIRFLOW_VAR_DAP_MIN_DATE_TRANSACTION_INITIAL +"' AS DATETIME) AS DATE) THEN CAST(LAST_DAY(CAST('"+ AIRFLOW_VAR_DAP_MIN_DATE_TRANSACTION_INITIAL +"' AS DATETIME)) AS DATE)\
                                                                                                    ELSE LAST_DAY(CAST(DATE_SUB('{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' , INTERVAL 1 MONTH) as DATE))\
                                                                                                END AS BUSINESS_DATE\
                                                                                        FROM (select 1 limit 1) \
                                                                                        WHERE 1=1 \
                                                                                            AND EXISTS ( \
                                                                                                            SELECT DISTINCT PROCESS_BUSINESS_DATE BUSINESS_DATE \
                                                                                                            FROM " + AIRFLOW_VAR_DAP_DATASET_TEMP_SP + "." + config_name.replace("_yaml","")+"_business_date_list" " \
                                                                                                            WHERE 1=1 \
                                                                                                                AND execution_date = CAST('{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' AS DATE) \
                                                                                                        ) \
                                                                                    ) SUB  \
                                                                                    ;"  
                                }
                , retries = 10
            )

        process_data_to_datamart = \
            BigQueryInsertJobOperator(
                task_id='process_data_to_datamart_{}'.format(config_name.replace("_yaml","")),
                configuration={
                    "query": {
                        "query":"call " + AIRFLOW_VAR_DAP_DATASET_SP + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='table_sp_name_" + config_name.replace("_yaml", "") + "') }} "+
                                " ( '" + AIRFLOW_VAR_DAP_JOB_TYPE + "'  \
                                    , '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                    , '" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "' \
                                    , '" + AIRFLOW_VAR_DAP_DATASET_DM + "' \
                                    , '{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}'  \
                                    , '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' \
                                    , '" + AIRFLOW_VAR_DAP_JOB_ID + "' \
                                    , '" + AIRFLOW_VAR_DAP_OMT_JOB_DEPENDENCY + "'\
                                    , '" + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + "'\
                                    , '" + AIRFLOW_VAR_DAP_DATASET_TEMP_SP + "'\
                                ) "
                        , "useLegacySql": False
                    }
                }
                , location= AIRFLOW_VAR_DAP_LOCATION
                , project_id= AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID
                , on_failure_callback=partial(function_master.error_handling, AIRFLOW_VAR_DAP_DAG_ID, "DATAMART_QUERY", AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + "." + config_name.replace("_yaml", ""))
                , retries = 10
            )
        
        #PROCESS AUTOFILL_BACKLOG
        autofill_backlog_insert_only = PythonOperator(
                task_id='autofill_backlog_insert_only_{}'.format(config_name.replace("_yaml",""))
                , python_callable=function_master.autofill_backlog_insert_only
                , on_failure_callback=partial(function_master.error_handling, AIRFLOW_VAR_DAP_DAG_ID, "DATAMART_QUERY", AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + "." + config_name.replace("_yaml", ""))
                , retries = AIRFLOW_VAR_DAP_RETRIES
                , retry_delay= AIRFLOW_VAR_DAP_RETRY_DELAY
                , op_kwargs={'insert_omt_backlog_insert_only_{}'.format(config_name.replace("_yaml","")) : "INSERT INTO `"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID +"`."+ AIRFLOW_VAR_DAP_OMT_PROCESS_LOG +"  \
                                                                                                            SELECT opl.JOB_TYPE  \
                                                                                                                , opl.JOB_NAME \
                                                                                                                , opl.START_DATE \
                                                                                                                , opl.END_DATE  \
                                                                                                                , CAST(list_tanggal.missing_execution_date|| ' 00:00:01' AS DATETIME) MAX_LAST_UPDATE_DATE \
                                                                                                                , opl.SRC_FILE_OR_TBL_NAME \
                                                                                                                , opl.TRG_TEMP_FILE \
                                                                                                                , opl.TRG_TBL_NAME \
                                                                                                                , opl.DELTA_DATA_COUNT \
                                                                                                                , opl.SRC_DATA_COUNT \
                                                                                                                , opl.TRG_DATA_COUNT \
                                                                                                                , 'DONE' \
                                                                                                                , 'AUTOFILL_BACKLOG' \
                                                                                                                , CAST(list_tanggal.missing_execution_date AS DATE) \
                                                                                                                , opl.JOB_ID  \
                                                                                                                , opl.BUSINESS_DATE \
                                                                                                            FROM OMT.OMT_PROCESS_LOG opl\
                                                                                                            , (SELECT SUBSTR(CAST(day_date AS STRING),1,10) missing_execution_date \
                                                                                                                    , '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' execution_date_reference\
                                                                                                                FROM `"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID +"`."+ AIRFLOW_VAR_DAP_OMT_CALENDAR_DATE +" \
                                                                                                                WHERE 1=1 \
                                                                                                                    and day_date < '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' \
                                                                                                                    and day_date >= DATE_TRUNC('{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' , MONTH) \
                                                                                                                    and day_date >= PARSE_DATETIME('%Y-%m-%d %H:%M:%E3S' , '" + AIRFLOW_VAR_DAP_MIN_DATE_TRANSACTION_INITIAL + "')  \
                                                                                                                    and CAST(day_date as DATE) not in ( \
                                                                                                                                                        SELECT EXECUTION_DATE \
                                                                                                                                                        FROM `"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID +"`."+ AIRFLOW_VAR_DAP_OMT_PROCESS_LOG +" a \
                                                                                                                                                        WHERE 1=1 \
                                                                                                                                                            AND TRG_TBL_NAME ='"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                                                                                                                            AND JOB_TYPE = '" + AIRFLOW_VAR_DAP_JOB_TYPE + "' \
                                                                                                                                                            AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                                                                                            AND SRC_FILE_OR_TBL_NAME ='DATAMART_QUERY' \
                                                                                                                                                            AND STATUS IN ('DONE')  \
                                                                                                                                                            AND IFNULL(STATUS_DESCRIPTION,'') NOT IN ('JOB_STATUS_FLAG') \
                                                                                                                                                            and EXECUTION_DATE BETWEEN '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' - " + AIRFLOW_VAR_DAP_OMT_AUTOFILL_BACKLOG_RETENTION +" and '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' \
                                                                                                                                                        ) \
                                                                                                            ) list_tanggal\
                                                                                                            WHERE 1=1 \
                                                                                                                AND EXECUTION_DATE = CAST(list_tanggal.execution_date_reference AS DATE) \
                                                                                                                AND JOB_TYPE = '" + AIRFLOW_VAR_DAP_JOB_TYPE + "' \
                                                                                                                AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                                                AND TRG_TBL_NAME = '"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM +".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                                                                                AND SRC_FILE_OR_TBL_NAME = 'DATAMART_QUERY' \
                                                                                                                AND IFNULL(STATUS_DESCRIPTION,'') <> 'JOB_STATUS_FLAG' \
                                                                                                                AND BUSINESS_DATE >= DATE_SUB(LAST_DAY(CAST(list_tanggal.execution_date_reference AS DATE)), INTERVAL 1 MONTH) \
                                                                                                            ;" 
                            }
            )
        
        delete_sandbox_data= BigQueryInsertJobOperator(
            task_id='delete_sandbox_data_{}'.format(config_name.replace("_yaml", ""))
            , configuration={
                                "query": {
                                            "query": "DELETE \
                                                    FROM `" + AIRFLOW_VAR_DAP_SANDBOX_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }} \
                                                    WHERE 1=1 \
                                                        AND BUSINESS_DATE = LAST_DAY(DATE_SUB(CAST('{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' AS DATE), INTERVAL 1 MONTH)) \
                                                        "
                                            , "useLegacySql": False
                                        }
                            }
            ,location=AIRFLOW_VAR_DAP_LOCATION
            ,project_id=AIRFLOW_VAR_DAP_SANDBOX_PROJECT_ID
            ,execution_timeout=timeout_treshold
            ,on_failure_callback=partial(function_master.error_handling, AIRFLOW_VAR_DAP_DAG_ID, "DATAMART_QUERY", AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + "." + config_name.replace("_yaml", ""))
            , retries = 10
        )

        send_to_sandbox = BigQueryInsertJobOperator(
            task_id='send_to_sandbox_{}'.format(config_name.replace("_yaml", ""))
            , configuration={
                                "query": {
                                            "query": "SELECT * \
                                                    FROM `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }} \
                                                    WHERE 1=1 \
                                                        AND BUSINESS_DATE = LAST_DAY(DATE_SUB(CAST('{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' AS DATE), INTERVAL 1 MONTH)) \
                                                        "
                                            , "useLegacySql": False
                                            , "destinationTable": {
                                                                    "projectId": AIRFLOW_VAR_DAP_SANDBOX_PROJECT_ID
                                                                    , "datasetId": AIRFLOW_VAR_DAP_DATASET_DM
                                                                    , "tableId": "{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}"
                                                                }
                                            , "writeDisposition": "WRITE_APPEND",  # Atur sesuai kebutuhan WRITE_TRUNCATE/WRITE_APPEND
                                        }
                            }
            ,location=AIRFLOW_VAR_DAP_LOCATION
            ,project_id=AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID
            ,execution_timeout=timeout_treshold
            ,on_failure_callback=partial(function_master.error_handling, AIRFLOW_VAR_DAP_DAG_ID, "DATAMART_QUERY", AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + "." + config_name.replace("_yaml", ""))
            , retries = 10
        )
        
        create_table_for_housekeeping = BigQueryOperator(
            task_id='create_table_for_housekeeping_{}'.format(config_name.replace("_yaml", ""))
            , sql="SELECT * \
                 FROM `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }} \
                 WHERE 1=1 \
                    AND BUSINESS_DATE < DATE_SUB(CURRENT_DATE() , INTERVAL {{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='retention_table_day_" + config_name.replace("_yaml", "") + "') }} DAY) \
                    AND 0 <> {{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='retention_table_day_" + config_name.replace("_yaml", "") + "') }} \
                ORDER BY 1"
            , destination_dataset_table= AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_TEMP_CLEANSING + "." + AIRFLOW_VAR_DAP_DATASET_DM + "_{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}_clean"  
            , write_disposition='WRITE_TRUNCATE'
            , create_disposition='CREATE_IF_NEEDED'
            , use_legacy_sql=False
            , retries = 10
        )

        housekeeping_data_to_archive = BashOperator(
            task_id='housekeeping_data_to_archive_{}'.format(config_name.replace("_yaml", ""))
            , bash_command= "bq extract \
                            --location=" + AIRFLOW_VAR_DAP_LOCATION + "\
                            --destination_format=CSV  \
                            --field_delimiter=',' " + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + ":" + AIRFLOW_VAR_DAP_DATASET_TEMP_CLEANSING + "." + AIRFLOW_VAR_DAP_DATASET_DM + "_{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}_clean gs://"+ AIRFLOW_VAR_DAP_ARCHIVED_BUCKET +"/{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='archive_gcs_folder_" + config_name.replace("_yaml", "") + "') }}/data/"+ AIRFLOW_VAR_DAP_DATASET_DM +"_{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}_clean_"+"{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}.csv"
            , retries = 10
        )

        delete_data_in_table_for_housekeeping = BigQueryInsertJobOperator(
                task_id='delete_data_in_table_for_housekeeping_{}'.format(config_name.replace("_yaml", ""))
               ,configuration={
                                "query": {
                                    "query": "DELETE \
                                                FROM `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }} \
                                                WHERE 1=1 \
                                                    AND BUSINESS_DATE < DATE_SUB(CURRENT_DATE() , INTERVAL {{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='retention_table_day_" + config_name.replace("_yaml", "") + "') }} DAY) \
                                                    AND 0 <> {{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='retention_table_day_" + config_name.replace("_yaml", "") + "') }} \
                                                ",
                                    "useLegacySql": False,
                                  }
                              }
               ,location=AIRFLOW_VAR_DAP_LOCATION
               ,project_id=AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID
               ,execution_timeout=timeout_treshold
               , retries = 10
            )
        
        
        update_omt_job_status_flag = BigQueryInsertJobOperator(
                task_id='update_omt_job_status_flag_{}'.format(config_name.replace("_yaml", ""))
               ,configuration={
                                "query": {
                                    "query": "UPDATE `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + '.' + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + "` \
                                            SET TRG_TEMP_FILE= 'N/A',DELTA_DATA_COUNT=0,SRC_DATA_COUNT=0,TRG_DATA_COUNT=0,STATUS='DONE',END_DATE=CURRENT_DATETIME('Asia/Jakarta'),MAX_LAST_UPDATE_DATE='1900-01-01 00:00:01'  \
                                            WHERE 1=1 \
                                            AND TRG_TBL_NAME ='" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                            AND JOB_TYPE = '" + AIRFLOW_VAR_DAP_JOB_TYPE + "' \
                                            AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                            AND SRC_FILE_OR_TBL_NAME ='DATAMART_QUERY' \
                                            AND STATUS in ('RUNNING') \
                                            AND STATUS_DESCRIPTION = 'JOB_STATUS_FLAG' \
                                            AND EXECUTION_DATE = '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' "
                                    , "useLegacySql": False,
                                  }
                              }
               ,location=AIRFLOW_VAR_DAP_LOCATION
               ,project_id=AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID
               ,execution_timeout=timeout_treshold
               , retries = 10
            )
        
        update_omt_job_master_status_flag = BigQueryInsertJobOperator(
                task_id='update_omt_job_master_status_flag_{}'.format(config_name.replace("_yaml", ""))
               ,configuration={
                                "query": {
                                    "query": "UPDATE `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + '.' + AIRFLOW_VAR_DAP_OMT_MASTER_JOB + "` \
                                            SET status_run_execution_date= '1'  \
                                            WHERE 1=1 \
                                                AND TABLE_NAME ='" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                AND ENABLE_FLAG = '1' \
                                        "
                                    , "useLegacySql": False,
                                  }
                              }
               ,location=AIRFLOW_VAR_DAP_LOCATION
               ,project_id=AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID
               ,execution_timeout=timeout_treshold
               , retries = 10
            )

        Start >> validate_master_job >> get_variable_omt_master_job >> validate_enable_flag_and_duplicate_run >> branch_validate >>  create_table_target >> create_table_sandbox >> update_if_error_exists >> delete_omt_error >> insert_omt_job_running >> autofill_backlog >> check_dependency_job_running >> check_dependency_datamart >> insert_omt_running >> process_data_to_datamart >> autofill_backlog_insert_only >> delete_sandbox_data >> send_to_sandbox >> create_table_for_housekeeping >> housekeeping_data_to_archive >> delete_data_in_table_for_housekeeping >> update_omt_job_status_flag >> update_omt_job_master_status_flag >> Finish
        branch_validate >> Finish