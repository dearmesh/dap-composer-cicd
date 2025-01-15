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

from DATA.utils.dap.load_config import ConfigFile
from DATA.operator.dap.function_sts_job_dmu_workspace_daily import Function

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
AIRFLOW_VAR_DAP_PATTERN_DELETED_FILE = Variable.get('AIRFLOW_VAR_DAP_PATTERN_DELETED_FILE')

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

AIRFLOW_VAR_DAP_JOB_TYPE = 'sts_job_daily'
AIRFLOW_VAR_DAP_DAG_ID = "sts_job_dmu_workspace_daily"
AIRFLOW_VAR_DAP_DAG_DESCRIPTION = "Cloudera DMU WORKSPACE to BQ DM_DAP Daily"
AIRFLOW_VAR_DAP_DAG_ALIAS = 'sts_job_dmu_workspace_daily'
AIRFLOW_VAR_DAP_DAG_PATH = '/DATA/config/dap/sts_job_dmu_workspace_daily/'
AIRFLOW_VAR_DAP_CATEGORY_JOB = 'STS'

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
    , tags=["STS","DAILY","DMU_WORKSPACE","DAP"]) as AIRFLOW_VAR_DAP_DAG_ALIAS:

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

        # VALIDATE RUNNING TYPE
        validate_running_type = PythonOperator (
            task_id='validate_running_type_{}'.format(config_name.replace("_yaml",""))
            , python_callable=function_master.initial_transaction
            , provide_context=True
            # execution_timeout=timeout_treshold,
            , op_kwargs={"initial_table_list_{}".format(config_name.replace("_yaml", "")) : "{{ dag_run.conf.get('initial_table_list','incremental') }}",
                        "schema_{}".format(config_name.replace("_yaml", "")):"bd_tableau" ,
                        "query_omt_{}".format(config_name.replace("_yaml", "")):"SELECT case when count(1) >= 1 then 1 else 0 end as COUNT_OMT \
                                                                                            FROM `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + " \
                                                                                            WHERE 1=1  \
                                                                                                AND STATUS = 'DONE' \
                                                                                                AND EXECUTION_DATE= '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}'  \
                                                                                                AND JOB_TYPE = '" + AIRFLOW_VAR_DAP_JOB_TYPE + "' \
                                                                                                AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                                AND TRG_TBL_NAME = '" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                                                                AND EXECUTION_DATE <= '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' \
                                                                                                AND EXECUTION_DATE BETWEEN DATE_SUB(CAST('{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' AS DATE) , INTERVAL " + AIRFLOW_VAR_DAP_OMT_CHECK_DT_IN_MONTH + " MONTH) \
                                                                                                AND CAST('{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' AS DATE) \
                                                                                "
                        }
            , retries= 5
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
               ,on_failure_callback=partial(function_master.error_handling, AIRFLOW_VAR_DAP_DAG_ID, config_name.replace("_yaml", "").replace("_", ".", 1))
               , retries = 10
            )
        
        # DELETE OMT PROCESS LOG dengan status ERROR
        delete_omt_error = PythonOperator(
            task_id='delete_omt_error_{}'.format(config_name.replace("_yaml", ""))
           ,python_callable=function_master.delete_omt
           ,on_failure_callback=partial(function_master.error_handling, AIRFLOW_VAR_DAP_DAG_ID, config_name.replace("_yaml", "").replace("_", ".", 1))
           ,op_kwargs={'query_{}'.format(config_name.replace("_yaml", "")): " DELETE `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + "`  \
                                                                              WHERE 1=1 \
                                                                                AND SRC_FILE_OR_TBL_NAME ='"  + config_name.replace("_yaml", "").replace("_", ".", 1) + "' \
                                                                                AND TRG_TBL_NAME ='" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                                                AND JOB_TYPE = '" + AIRFLOW_VAR_DAP_JOB_TYPE + "' \
                                                                                AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                AND STATUS in ('ERROR') \
                                                                              ;"
                        , 'query2_{}'.format(config_name.replace("_yaml", "")): " DELETE `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + "`  \
                                                                              WHERE 1=1 \
                                                                                AND SRC_FILE_OR_TBL_NAME ='"  + config_name.replace("_yaml", "").replace("_", ".", 1) + "' \
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
           ,on_failure_callback=partial(function_master.error_handling, AIRFLOW_VAR_DAP_DAG_ID, config_name.replace("_yaml", "").replace("_", ".", 1))
           ,op_kwargs={'query_{}'.format(config_name.replace("_yaml", "")): " INSERT INTO `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + "  \
                                                                                  VALUES ('" + AIRFLOW_VAR_DAP_JOB_TYPE + "'  \
                                                                                          , '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                          , CURRENT_DATETIME('Asia/Jakarta') \
                                                                                          , NULL  \
                                                                                          , NULL   \
                                                                                          , '"  + config_name.replace("_yaml", "").replace("_", ".", 1) + "'  \
                                                                                          , 'N/A'  \
                                                                                          , '" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}'   \
                                                                                          , 0   \
                                                                                          , 0  \
                                                                                          , 0   \
                                                                                          , 'RUNNING'   \
                                                                                          , 'JOB_STATUS_FLAG' \
                                                                                          , '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}'  \
                                                                                          , '" + AIRFLOW_VAR_DAP_JOB_ID + "' \
                                                                                          , CAST('{{ (execution_date - macros.timedelta(days=1)).in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' AS DATE) \
                                                                                          ); "
                        }
            , retries = 10
        )

        #PROCESS AUTOFILL_BACKLOG NO PROCESS
        autofill_backlog_process = PythonOperator(
            task_id='autofill_backlog_{}'.format(config_name.replace("_yaml",""))
            ,python_callable=function_master.autofill_backlog
            ,on_failure_callback=partial(function_master.error_handling, AIRFLOW_VAR_DAP_DAG_ID, config_name.replace("_yaml", "").replace("_", ".", 1))
            ,op_kwargs={'query_insert_backlog_{}'.format(config_name.replace("_yaml", "")): "INSERT INTO `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + "  \
                                                                                            SELECT '" + AIRFLOW_VAR_DAP_JOB_TYPE + "'  \
                                                                                                    , '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                                    , CURRENT_DATETIME('Asia/Jakarta') \
                                                                                                    , CURRENT_DATETIME('Asia/Jakarta')  \
                                                                                                    , IFNULL(ludt.LAST_UPDATE_DATE,'1900-01-01 00:00:01.000')   MAX_LAST_UPDATE_DATE\
                                                                                                    , '"  + config_name.replace("_yaml", "").replace("_", ".", 1) + "'  \
                                                                                                    , 'N/A'  \
                                                                                                    , '" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}'   \
                                                                                                    , 0   \
                                                                                                    , 0  \
                                                                                                    , 0   \
                                                                                                    , 'DONE'   \
                                                                                                    , 'AUTOFILL_BACKLOG' \
                                                                                                    , CAST(list_tanggal.missing_execution_date AS DATE)  \
                                                                                                    , '" + AIRFLOW_VAR_DAP_JOB_ID + "'  \
                                                                                                    , DATE_SUB(CAST(list_tanggal.missing_execution_date AS DATE),INTERVAL 1 DAY) \
                                                                                            FROM (SELECT max(MAX_LAST_UPDATE_DATE) LAST_UPDATE_DATE \
                                                                                                    FROM `"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID +"`."+ AIRFLOW_VAR_DAP_OMT_PROCESS_LOG +" \
                                                                                                    WHERE 1=1 \
                                                                                                            AND SRC_FILE_OR_TBL_NAME ='"  + config_name.replace("_yaml", "").replace("_", ".", 1) + "' \
                                                                                                            AND TRG_TBL_NAME = '"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM +".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                                                                            AND JOB_TYPE = '" + AIRFLOW_VAR_DAP_JOB_TYPE + "' \
                                                                                                            AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                                            AND STATUS = 'DONE' \
                                                                                                            AND EXECUTION_DATE < CAST('{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' AS DATE) \
                                                                                                    ) ludt \
                                                                                                    , (select SUBSTR(CAST(day_date AS STRING),1,10) missing_execution_date \
                                                                                                        from `"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID +"`."+ AIRFLOW_VAR_DAP_OMT_CALENDAR_DATE +" \
                                                                                                        where 1=1 \
                                                                                                            and day_date < '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' \
                                                                                                            and day_date >= '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' - " + AIRFLOW_VAR_DAP_OMT_AUTOFILL_BACKLOG_RETENTION + "   \
                                                                                                            and day_date >= PARSE_DATETIME('%Y-%m-%d %H:%M:%E3S' , '" + AIRFLOW_VAR_DAP_MIN_DATE_TRANSACTION_INITIAL + "')  \
                                                                                                            and CAST(day_date as DATE) not in ( \
                                                                                                                                            select EXECUTION_DATE \
                                                                                                                                            from `"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID +"`."+ AIRFLOW_VAR_DAP_OMT_PROCESS_LOG +" a \
                                                                                                                                            where 1=1 \
                                                                                                                                                AND TRG_TBL_NAME ='"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM +".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                                                                                                                                AND JOB_TYPE = '" + AIRFLOW_VAR_DAP_JOB_TYPE + "' \
                                                                                                                                                AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                                                                                AND SRC_FILE_OR_TBL_NAME ='"  +  config_name.replace("_yaml","").replace("_",".",1)  + "' \
                                                                                                                                                and EXECUTION_DATE BETWEEN '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' - " + AIRFLOW_VAR_DAP_OMT_AUTOFILL_BACKLOG_RETENTION +" and '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' \
                                                                                                                                        ) \
                                                                                                        ) list_tanggal \
                                                                                            WHERE 1=1 \
                                                                                            ;"
                    }
                , retries = 5
            )
        
        # INSERT OMT PROCESS LOG dengan status Running
        insert_omt_running = PythonOperator(
            task_id='insert_omt_running_{}'.format(config_name.replace("_yaml",""))
            , python_callable=function_master.insert_omt
            ,op_kwargs={'query_{}'.format(config_name.replace("_yaml", "")): " INSERT INTO `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + "  \
                                                                                    VALUES ('" + AIRFLOW_VAR_DAP_JOB_TYPE + "'  \
                                                                                            , '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                                                                            , CURRENT_DATETIME('Asia/Jakarta') \
                                                                                            , NULL  \
                                                                                            , NULL   \
                                                                                            , '"  + config_name.replace("_yaml", "").replace("_", ".", 1) + "'  \
                                                                                            , 'N/A'  \
                                                                                            , '" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}'   \
                                                                                            , 0   \
                                                                                            , 0  \
                                                                                            , 0   \
                                                                                            , 'RUNNING'   \
                                                                                            , '' \
                                                                                            , '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}'  \
                                                                                            , '" + AIRFLOW_VAR_DAP_JOB_ID + "' \
                                                                                            , CAST('{{ (execution_date - macros.timedelta(days=1)).in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}' AS DATE) \
                                                                                            ); "
                            }
            , retries = AIRFLOW_VAR_DAP_RETRIES
            )
        
        run_transfer = CloudDataTransferServiceRunJobOperator(
            task_id='running_storage_transfer_{}'.format(config_name.replace("_yaml", "")),
            job_name="transferJobs/{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='sts_job_name_" + config_name.replace("_yaml", "") + "') }}_{{ task_instance.xcom_pull(task_ids='validate_running_type_" + config_name.replace("_yaml", "") + "',key='check_" + config_name.replace("_yaml", "") + "') }}",
            project_id=AIRFLOW_VAR_DAP_LANDING_PROJECT_ID,
            on_failure_callback=partial(function_master.error_handling, AIRFLOW_VAR_DAP_DAG_ID, config_name.replace("_yaml", "").replace("_", ".", 1))
        )

        check_sts_sensor = PythonOperator(
            task_id='check_sts_sensor_{}'.format(config_name.replace("_yaml", "")),
            python_callable=function_master.sensor_sts,
            provide_context=True,
            on_failure_callback=partial(function_master.error_handling, AIRFLOW_VAR_DAP_DAG_ID, config_name.replace("_yaml", "").replace("_", ".", 1)),
            op_kwargs={'transferOperations' : "{{ task_instance.xcom_pull(task_ids='running_storage_transfer_"+config_name.replace("_yaml", "")+ "',key='return_value')['name']}}"}
        )

        update_omt_success = PythonOperator (
                task_id = 'update_omt_success_{}'.format(config_name.replace("_yaml","")) 
               ,python_callable=function_master.update_omt_on_success
               ,on_failure_callback=partial(function_master.error_handling, AIRFLOW_VAR_DAP_DAG_ID, config_name.replace("_yaml", "").replace("_", ".", 1))
               ,op_kwargs={'query_source_{}'.format(config_name.replace("_yaml","")) : " SELECT COUNT(1) AS count_number "
                                                                                       " FROM "  + config_name.replace("_yaml","").replace("_",".",1) + " ",
                           'query_target_{}'.format(config_name.replace("_yaml","")) : " SELECT COUNT(1) as count_number , CURRENT_DATETIME('Asia/Jakarta') as max_update_date "
                                                                                       " FROM `"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "`." + AIRFLOW_VAR_DAP_DATASET_MISPLUS +".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}"
                                                                                       " WHERE 1=1 " 
                                                                                       " AND ACTIVE_FLAG ='Y'",
                           'query_update_bq_{}'.format(config_name.replace("_yaml","")) : " UPDATE `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID +'.'+ AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + "`"
                                                                                          " SET DELTA_DATA_COUNT={DELTA_DATA_COUNT},STATUS='DONE',END_DATE=CURRENT_DATETIME('Asia/Jakarta'),MAX_LAST_UPDATE_DATE=CURRENT_DATETIME('Asia/Jakarta') "
                                                                                          " WHERE 1=1 "
                                                                                          " AND SRC_FILE_OR_TBL_NAME ='"  + config_name.replace("_yaml","").replace("_",".",1) + "' "
                                                                                          " AND TRG_TBL_NAME ='"+ AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM +".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}'"
                                                                                          " AND JOB_TYPE = '" + AIRFLOW_VAR_DAP_JOB_TYPE + "'"
                                                                                          " AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "'"
                                                                                          " AND STATUS = 'RUNNING'"
                                                                                          " AND IFNULL(STATUS_DESCRIPTION,'') <> 'JOB_STATUS_FLAG' "
                                                                                          " AND EXECUTION_DATE = '{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}'"
                         }
                , retries = 10
            )
        
        #Delete Trash Files
        delete_trash_file = BashOperator(
            task_id='delete_trash_file_{}'.format(config_name.replace("_yaml", ""))
            , retries = 10
            , trigger_rule='none_failed_or_skipped'
            , bash_command=" \
                gsutil -m  rm -rf `gsutil -m ls -R  gs://"+ AIRFLOW_VAR_DAP_LANDING_BUCKET+"/{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='source_gcs_folder_" + config_name.replace("_yaml", "") + "') }} | grep "+f"\"{AIRFLOW_VAR_DAP_PATTERN_DELETED_FILE}\"` \
            ;exit 0"
        )
        
        #Housekeeping File in Archive
        move_files_from_landing_to_archive = BashOperator(
            task_id='move_files_from_landing_to_archive_{}'.format(config_name.replace("_yaml", ""))
            , retries = 10
            , trigger_rule='none_failed_or_skipped'
            , bash_command=" \
            gsutil ls -l gs://"+AIRFLOW_VAR_DAP_LANDING_BUCKET+"/{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='source_gcs_folder_" + config_name.replace("_yaml", "") + "') }} |" +
            'grep -v "TOTAL" |' + 
            "awk '{print $2, $3}' |" +
            "while read time filename; do \
                if [[ $(date -d '$time' +%s) -lt $(date -d '{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='retention_file_day_" + config_name.replace("_yaml", "") + "') }} days ago' +%s) ]]; then \
                    filename=$(echo '$filename' |"+ "awk -F/ '{print $NF}');" +
                    " gsutil -m mv 'gs://" + AIRFLOW_VAR_DAP_LANDING_BUCKET + "/{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='source_gcs_folder_" + config_name.replace("_yaml", "") + "') }}/$filename' 'gs://" + AIRFLOW_VAR_DAP_ARCHIVED_BUCKET + "/{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='archive_gcs_folder_" + config_name.replace("_yaml", "") + "') }}/$filename'; \
                fi \
            done ;exit 0"
        )

        # CREATE TABLE TARGET
        create_table_target = \
            BigQueryInsertJobOperator(
                task_id='create_table_target_{}'.format(config_name.replace("_yaml","")),
                configuration={
                    "query": {
                        "query": xfer_config.main_bigquery_table.schema_fields.replace('v_dataset_id',AIRFLOW_VAR_DAP_DATASET_DM).replace('v_connection',AIRFLOW_VAR_DAP_CONNECTION_BIGLAKE).replace('v_bucket',AIRFLOW_VAR_DAP_LANDING_BUCKET+"/{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='source_gcs_folder_" + config_name.replace("_yaml", "") + "') }}"),
                        "useLegacySql": False,
                    }
                },
                location= AIRFLOW_VAR_DAP_LOCATION,
                project_id= AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID
                , retries= 5
                # execution_timeout=timeout_treshold
            )
        
        update_omt_job_status_flag = BigQueryInsertJobOperator(
                task_id='update_omt_job_status_flag_{}'.format(config_name.replace("_yaml", ""))
               ,configuration={
                                "query": {
                                    "query": "UPDATE `" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + '.' + AIRFLOW_VAR_DAP_OMT_PROCESS_LOG + "` \
                                            SET TRG_TEMP_FILE= 'N/A',DELTA_DATA_COUNT=0,SRC_DATA_COUNT=0,TRG_DATA_COUNT=0,STATUS='DONE',END_DATE=CURRENT_DATETIME('Asia/Jakarta'),MAX_LAST_UPDATE_DATE='1900-01-01 00:00:01'  \
                                            WHERE 1=1 \
                                            AND SRC_FILE_OR_TBL_NAME ='"  + config_name.replace("_yaml", "").replace("_", ".", 1) + "' \
                                            AND TRG_TBL_NAME ='" + AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + "." + AIRFLOW_VAR_DAP_DATASET_DM + ".{{ task_instance.xcom_pull(task_ids='get_variable_omt_master_job_" + config_name.replace("_yaml", "") + "',key='target_table_name_" + config_name.replace("_yaml", "") + "') }}' \
                                            AND JOB_TYPE = '" + AIRFLOW_VAR_DAP_JOB_TYPE + "' \
                                            AND JOB_NAME = '" + AIRFLOW_VAR_DAP_DAG_ID + "' \
                                            AND STATUS in ('DONE','RUNNING') \
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

        Start >> validate_master_job >> get_variable_omt_master_job >> validate_running_type >> validate_enable_flag_and_duplicate_run >> branch_validate >> update_if_error_exists >> delete_omt_error >> insert_omt_job_running >> insert_omt_running >> run_transfer >> check_sts_sensor >> autofill_backlog_process >> update_omt_success >> delete_trash_file >> move_files_from_landing_to_archive >> create_table_target >> update_omt_job_status_flag >> update_omt_job_master_status_flag >> Finish
        branch_validate >> Finish