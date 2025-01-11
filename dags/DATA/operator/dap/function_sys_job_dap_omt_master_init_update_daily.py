from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.api.common.experimental.get_task_instance import get_task_instance
import pytz
from datetime import datetime, timedelta


import re

class Function():
    def __init__(
                self
                ,AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID
                ,AIRFLOW_VAR_DAP_LANDING_PROJECT_ID
                ,AIRFLOW_VAR_DAP_LANDING_BUCKET
                ,AIRFLOW_VAR_DAP_ARCHIVED_BUCKET
                 ,AIRFLOW_VAR_DAP_EXPORT_FORMAT
                 ,AIRFLOW_VAR_DAP_OMT_PROCESS_LOG
                 ,AIRFLOW_VAR_DAP_OMT_ERROR_LOG
                 ,AIRFLOW_VAR_DAP_OMT_JOB_DEPENDENCY
                 ,AIRFLOW_VAR_DAP_OMT_CALENDAR_DATE
                 ,AIRFLOW_VAR_DAP_OMT_AUTOFILL_BACKLOG_RETENTION
                 ,AIRFLOW_VAR_DAP_LOCATION
                 ,AIRFLOW_VAR_DAP_EMAIL_SEND_TO
                 ,AIRFLOW_VAR_DAP_JOB_TYPE
                 ,AIRFLOW_VAR_DAP_DAG_ID
                 ,AIRFLOW_VAR_DAP_DAG_DESCRIPTION
                 ,AIRFLOW_VAR_DAP_DAG_ALIAS
                 ,AIRFLOW_VAR_DAP_DAG_PATH
                 ):
        self.AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID=AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID
        self.AIRFLOW_VAR_DAP_LANDING_PROJECT_ID=AIRFLOW_VAR_DAP_LANDING_PROJECT_ID
        self.AIRFLOW_VAR_DAP_LANDING_BUCKET=AIRFLOW_VAR_DAP_LANDING_BUCKET
        self.AIRFLOW_VAR_DAP_ARCHIVED_BUCKET=AIRFLOW_VAR_DAP_ARCHIVED_BUCKET
        self.AIRFLOW_VAR_DAP_EXPORT_FORMAT=AIRFLOW_VAR_DAP_EXPORT_FORMAT
        self.AIRFLOW_VAR_DAP_OMT_PROCESS_LOG=AIRFLOW_VAR_DAP_OMT_PROCESS_LOG
        self.AIRFLOW_VAR_DAP_OMT_ERROR_LOG=AIRFLOW_VAR_DAP_OMT_ERROR_LOG
        self.AIRFLOW_VAR_DAP_OMT_JOB_DEPENDENCY=AIRFLOW_VAR_DAP_OMT_JOB_DEPENDENCY
        self.AIRFLOW_VAR_DAP_OMT_CALENDAR_DATE=AIRFLOW_VAR_DAP_OMT_CALENDAR_DATE
        self.AIRFLOW_VAR_DAP_OMT_AUTOFILL_BACKLOG_RETENTION=AIRFLOW_VAR_DAP_OMT_AUTOFILL_BACKLOG_RETENTION
        self.AIRFLOW_VAR_DAP_LOCATION=AIRFLOW_VAR_DAP_LOCATION
        self.AIRFLOW_VAR_DAP_EMAIL_SEND_TO=AIRFLOW_VAR_DAP_EMAIL_SEND_TO
        self.AIRFLOW_VAR_DAP_JOB_TYPE=AIRFLOW_VAR_DAP_JOB_TYPE
        self.AIRFLOW_VAR_DAP_DAG_ID=AIRFLOW_VAR_DAP_DAG_ID
        self.AIRFLOW_VAR_DAP_DAG_DESCRIPTION=AIRFLOW_VAR_DAP_DAG_DESCRIPTION
        self.AIRFLOW_VAR_DAP_DAG_ALIAS=AIRFLOW_VAR_DAP_DAG_ALIAS
        self.AIRFLOW_VAR_DAP_DAG_PATH=AIRFLOW_VAR_DAP_DAG_PATH

    def finish(self,**kwargs):
            for task_instance in kwargs['dag_run'].get_task_instances():

                print('task_intance :' + str(task_instance.task_id))
                print('task_intance.current_state :' + str(task_instance.current_state))

                if task_instance.current_state() != 'success' and \
                    task_instance.current_state() != 'skipped' and \
                    task_instance.current_state() != 'removed' and \
                    task_instance.task_id != kwargs['task_instance'].task_id:
                        print('kwargtask instance :' + kwargs['task_instance'].task_id)
                        print('task instance state :' + task_instance.current_state())
                        print('task instance :' + task_instance.task_id)
                        raise Exception("Task {} failed. Failing this DAG run".format(task_instance.task_id))

    