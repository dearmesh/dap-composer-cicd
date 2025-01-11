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
        
    def branch_validate_enable_flag_and_duplicate_run(self,**kwargs): 
        
        count_main_process = kwargs['ti'].xcom_pull(task_ids='validate_enable_flag_and_duplicate_run_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:])),key='count_main_process')
        check_enable_flag = kwargs['ti'].xcom_pull(task_ids='validate_enable_flag_and_duplicate_run_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:])),key='check_enable_flag')
        if count_main_process == 0 and check_enable_flag == 1 :
            return 'create_table_target_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:]))
        else:
            return 'Finish'
        
    def delete_omt(self,**kwargs):
        
        config={
            "job_type":"Query",
            "query":{
                "query":kwargs['query_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[3:]))],
                "useLegacySql":False,
                # "allow_large_results":True,
                "location":self.AIRFLOW_VAR_DAP_LOCATION
            }}
        
        config2={
            "job_type":"Query",
            "query":{
                "query":kwargs['query2_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[3:]))],
                "useLegacySql":False,
                # "allow_large_results":True,
                "location":self.AIRFLOW_VAR_DAP_LOCATION
            }}

        print(str(config["query"]["query"]))

        hook = BigQueryHook(gcp_conn_id='google_cloud_default', delegate_to=None, use_legacy_sql=False, location=self.AIRFLOW_VAR_DAP_LOCATION)
        delete_query=hook.insert_job(configuration=config, project_id=self.AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID, nowait=False)
        
        print(str(config2["query"]["query"]))
        delete_query2=hook.insert_job(configuration=config2, project_id=self.AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID, nowait=False)

    def insert_omt(self,**kwargs):
        
        print(kwargs['query_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[3:]))])

        config={
            "job_type":"Query",
            "query":{
                "query":kwargs['query_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[3:]))],
                "useLegacySql":False,
                # "allow_large_results":True,
                "location":self.AIRFLOW_VAR_DAP_LOCATION
            }}

        hook = BigQueryHook(gcp_conn_id='google_cloud_default', delegate_to=None, use_legacy_sql=False,location=self.AIRFLOW_VAR_DAP_LOCATION)
        target_count=hook.insert_job(configuration=config, project_id=self.AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID, nowait=False)

    def insert_omt_job_running(self,**kwargs):

        print(kwargs['query_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[4:]))])

        config={
            "job_type":"Query",
            "query":{
                "query":kwargs['query_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[4:]))],
                "useLegacySql":False,
                # "allow_large_results":True,
                "location":self.AIRFLOW_VAR_DAP_LOCATION
            }}

        hook = BigQueryHook(gcp_conn_id='google_cloud_default', delegate_to=None, use_legacy_sql=False, location=self.AIRFLOW_VAR_DAP_LOCATION)
        target_count=hook.insert_job(configuration=config, project_id=self.AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID, nowait=False)
        

    def error_handling(self,job_name,src_or_tbl_name,table_target,context):
        
        # Initiate Connection Hook
        bigquery_hook = BigQueryHook(gcp_conn_id='google_cloud_default', delegate_to=None, use_legacy_sql=False,location='asia-southeast2')
        # gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')

        query_update_omt = 'update `'+ self.AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + '.' + self.AIRFLOW_VAR_DAP_OMT_PROCESS_LOG  +'` \
                            set STATUS="ERROR" ,END_DATE=CURRENT_DATETIME("Asia/Jakarta") ,STATUS_DESCRIPTION="{exception}"  \
                            WHERE 1=1 \
                                AND SRC_FILE_OR_TBL_NAME ="{src_or_tbl_name}"  \
                                AND execution_date="{execution_date}" \
                                AND JOB_NAME = "{job_name}" \
                                AND TRG_TBL_NAME = "{table_target}"\
                                AND STATUS="RUNNING"'
        
        query_insert_error_omt = 'insert into `'+ self.AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + '.' + self.AIRFLOW_VAR_DAP_OMT_ERROR_LOG  +'` \
                                SELECT JOB_TYPE \
                                        , JOB_NAME \
                                        , START_DATE \
                                        , CURRENT_DATETIME("Asia/Jakarta") \
                                        , SRC_FILE_OR_TBL_NAME \
                                        , TRG_TBL_NAME \
                                        , "ERROR" \
                                        , "{exception}" \
                                        , execution_date \
                                        , JOB_ID \
                                        , business_date \
                                FROM  `'+ self.AIRFLOW_VAR_DAP_PROCESSING_PROJECT_ID + '.' + self.AIRFLOW_VAR_DAP_OMT_PROCESS_LOG  +'` \
                                WHERE 1=1 \
                                    AND SRC_FILE_OR_TBL_NAME ="{src_or_tbl_name}"  \
                                    AND execution_date="{execution_date}" \
                                    AND JOB_NAME = "{job_name}" \
                                    AND TRG_TBL_NAME = "{table_target}"\
                                    AND STATUS="ERROR"'

        # update error log
        bigquery_hook.run(sql=query_update_omt.format(exception=str(context.get('exception')).replace('\n',' ').replace('\"','\'') ,job_name=job_name ,src_or_tbl_name=src_or_tbl_name,table_target=table_target ,execution_date=str(context['execution_date'])[0:10]))

        # insert into omt error log
        bigquery_hook.run(sql=query_insert_error_omt.format(exception=str(context.get('exception')).replace('\n',' ').replace('\"','\'') ,job_name=job_name ,src_or_tbl_name=src_or_tbl_name,table_target=table_target ,execution_date=str(context['execution_date'])[0:10]))

    # Define the function to get the current date and time in Asia
    def get_current_time(self):
        asia_tz = pytz.timezone('Asia/Jakarta') # Set the timezone to Asia/Kolkata
        current_time = datetime.now(asia_tz) # Get the current time in the timezone
        current_time_str = current_time.strftime('%Y%m%d%H%M%S') # Convert the time to the desired format
        return current_time_str
    
    def validate_enable_flag_and_duplicate_run(self, **kwargs):

        # Initiate Connection Hook
        bigquery_hook = BigQueryHook(gcp_conn_id='google_cloud_default', delegate_to=None,use_legacy_sql=False, location=self.AIRFLOW_VAR_DAP_LOCATION)

        # get_first -> list/ tuple
        check_status_pass_result = bigquery_hook.get_first(sql=kwargs['query_validate_{}'.format("_".join(kwargs['task_instance'].task_id.split("_")[6:]))])
        kwargs['ti'].xcom_push(key='count_main_process', value=check_status_pass_result[0])
        kwargs['ti'].xcom_push(key='check_enable_flag', value=check_status_pass_result[1])
  
    def autofill_backlog(self,**kwargs):

        bigquery_hook = BigQueryHook(gcp_conn_id='google_cloud_default', delegate_to=None, use_legacy_sql=False,location=self.AIRFLOW_VAR_DAP_LOCATION)

        print(kwargs['query_backlog_data_with_process_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:]))])
        v_missing_execution_date = bigquery_hook.get_records(sql=kwargs['query_backlog_data_with_process_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:]))])
        
        ## Autofill Backlog With Process
        for execution_date in v_missing_execution_date:
            if execution_date :       

                print(kwargs['update_if_error_exists_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:]))].format(execution_date=datetime.strptime(execution_date[0],"%Y-%m-%d").date()))

                bigquery_hook.run(sql=kwargs['update_if_error_exists_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:]))].format(execution_date=datetime.strptime(execution_date[0],"%Y-%m-%d").date()))


                print(kwargs['delete_omt_error_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:]))].format(execution_date=datetime.strptime(execution_date[0],"%Y-%m-%d").date()))

                bigquery_hook.run(sql=kwargs['delete_omt_error_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:]))].format(execution_date=datetime.strptime(execution_date[0],"%Y-%m-%d").date()))
     

                print(kwargs['check_dependency_backlog_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:]))].format(datetime.strptime(execution_date[0],"%Y-%m-%d").date())) 

                bigquery_hook.run(sql=kwargs['check_dependency_backlog_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:]))].format(datetime.strptime(execution_date[0],"%Y-%m-%d").date())) 


                print(kwargs['insert_omt_backlog_with_process_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:]))].format(execution_date=datetime.strptime(execution_date[0],"%Y-%m-%d").date(),last_day_business_date=datetime.strptime(execution_date[1],"%Y-%m-%d").date()))

                bigquery_hook.run(sql=kwargs['insert_omt_backlog_with_process_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:]))].format(execution_date=datetime.strptime(execution_date[0],"%Y-%m-%d").date(),last_day_business_date=datetime.strptime(execution_date[1],"%Y-%m-%d").date()))

                
                print(kwargs['query_insert_backlog_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:]))].format(datetime.strptime(execution_date[0],"%Y-%m-%d").date())) 

                bigquery_hook.run(sql=kwargs['query_insert_backlog_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:]))].format(datetime.strptime(execution_date[0],"%Y-%m-%d").date())) 

                
                print(kwargs['insert_omt_backlog_insert_only_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:]))].format(execution_date=datetime.strptime(execution_date[0],"%Y-%m-%d").date()))

                bigquery_hook.run(sql=kwargs['insert_omt_backlog_insert_only_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:]))].format(execution_date=datetime.strptime(execution_date[0],"%Y-%m-%d").date()))
 

                print(kwargs['delete_sandbox_data_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:]))].format(execution_date=datetime.strptime(execution_date[0],"%Y-%m-%d").date()))

                bigquery_hook.run(sql=kwargs['delete_sandbox_data_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:]))].format(execution_date=datetime.strptime(execution_date[0],"%Y-%m-%d").date()))
 

                print(kwargs['send_to_sandbox_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:]))].format(execution_date=datetime.strptime(execution_date[0],"%Y-%m-%d").date()))

                bigquery_hook.run(sql=kwargs['send_to_sandbox_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[2:]))].format(execution_date=datetime.strptime(execution_date[0],"%Y-%m-%d").date()))
 

    def autofill_backlog_insert_only(self,**kwargs):

        bigquery_hook = BigQueryHook(gcp_conn_id='google_cloud_default', delegate_to=None, use_legacy_sql=False,location=self.AIRFLOW_VAR_DAP_LOCATION)

        ##  Autofillbacklog Insert Only
        # print(kwargs['query_backlog_data_insert_only_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[4:]))])
        
        # v_missing_execution_date_insert_only = bigquery_hook.get_records(sql=kwargs['query_backlog_data_insert_only_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[4:]))])

        # for execution_date_insert_only in v_missing_execution_date_insert_only:
        #     if execution_date_insert_only :

        print(kwargs['insert_omt_backlog_insert_only_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[4:]))])

        bigquery_hook.run(sql=kwargs['insert_omt_backlog_insert_only_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[4:]))])
 

    def get_variable_omt_master_job(self,**kwargs):
        
        # Initiate Connection Hook
        bigquery_hook = BigQueryHook(gcp_conn_id='google_cloud_default', delegate_to=None, use_legacy_sql=False,location=self.AIRFLOW_VAR_DAP_LOCATION)
        
        # get_first -> list/ tuple
        variable_omt_master_job = bigquery_hook.get_first(sql=kwargs['query_get_var_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[5:]))])

        kwargs['ti'].xcom_push(key='job_name_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[5:])), value=variable_omt_master_job[0])
        kwargs['ti'].xcom_push(key='table_name_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[5:])), value=variable_omt_master_job[1])
        kwargs['ti'].xcom_push(key='query_parameter_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[5:])), value=variable_omt_master_job[2])
        kwargs['ti'].xcom_push(key='execution_date_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[5:])), value=variable_omt_master_job[3])
        kwargs['ti'].xcom_push(key='status_run_execution_date_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[5:])), value=variable_omt_master_job[4])
        kwargs['ti'].xcom_push(key='category_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[5:])), value=variable_omt_master_job[5])
        kwargs['ti'].xcom_push(key='enable_flag_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[5:])), value=variable_omt_master_job[6])
        kwargs['ti'].xcom_push(key='prioritas_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[5:])), value=variable_omt_master_job[7])
        kwargs['ti'].xcom_push(key='source_gcs_folder_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[5:])), value=variable_omt_master_job[8])
        kwargs['ti'].xcom_push(key='archive_gcs_folder_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[5:])), value=variable_omt_master_job[9])
        kwargs['ti'].xcom_push(key='retention_file_day_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[5:])), value=variable_omt_master_job[10])
        kwargs['ti'].xcom_push(key='retention_table_day_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[5:])), value=variable_omt_master_job[11])
        kwargs['ti'].xcom_push(key='query_create_table_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[5:])), value=variable_omt_master_job[12])
        kwargs['ti'].xcom_push(key='target_table_name_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[5:])), value=variable_omt_master_job[13])
        kwargs['ti'].xcom_push(key='table_sp_name_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[5:])), value=variable_omt_master_job[14])
        kwargs['ti'].xcom_push(key='sts_job_name_{}'.format( "_".join(kwargs['task_instance'].task_id.split("_")[5:])), value=variable_omt_master_job[15])