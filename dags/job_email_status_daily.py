from airflow import settings
from datetime import datetime, timedelta
from functools import partial
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
# from airflow.operators.oracle_operator import OracleOperator
# from airflow.hooks.oracle_hook import OracleHook
# from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
# from airflow.providers.google.cloud.transfers.oracle_to_gcs import OracleToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import (BigQueryInsertJobOperator , BigQueryValueCheckOperator)
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.api.common.experimental.get_task_instance import get_task_instance
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.email_operator import EmailOperator

import re
import os
import pytz
import pendulum

local_tz = pendulum.timezone("Asia/Jakarta")

_BASE_FOLDER = settings.DAGS_FOLDER
timeout_treshold = timedelta(minutes=5)

#Initial Variable
AIRFLOW_VAR_PROJECT_ID = Variable.get('AIRFLOW_VAR_PROJECT_ID')
AIRFLOW_VAR_PROCESS_BUCKET = Variable.get('AIRFLOW_VAR_PROCESS_BUCKET')
AIRFLOW_VAR_EXPORT_FORMAT = Variable.get('AIRFLOW_VAR_EXPORT_FORMAT')
AIRFLOW_VAR_OMT_PROCESS_LOG = Variable.get('AIRFLOW_VAR_OMT_PROCESS_LOG')
AIRFLOW_VAR_OMT_PROCESS_DELETE_LOG = Variable.get('AIRFLOW_VAR_OMT_PROCESS_DELETE_LOG')
AIRFLOW_VAR_OMT_ERROR_LOG = Variable.get('AIRFLOW_VAR_OMT_ERROR_LOG')
AIRFLOW_VAR_OMT_DEPENDENCY = Variable.get('AIRFLOW_VAR_OMT_DEPENDENCY')
AIRFLOW_VAR_OMT_DELETE_LOG_ORA = Variable.get('AIRFLOW_VAR_OMT_DELETE_LOG_ORA')
AIRFLOW_VAR_OMT_DELETE_LOG_SAFE = Variable.get('AIRFLOW_VAR_OMT_DELETE_LOG_SAFE')
AIRFLOW_VAR_LOCATION = Variable.get('AIRFLOW_VAR_LOCATION')
AIRFLOW_VAR_OMT_CALENDAR_DATE = Variable.get('AIRFLOW_VAR_OMT_CALENDAR_DATE')
AIRFLOW_VAR_OMT_AUTOFILL_BACKLOG_RETENTION = Variable.get('AIRFLOW_VAR_OMT_AUTOFILL_BACKLOG_RETENTION')
AIRFLOW_VAR_EMAIL_SEND_TO = Variable.get('AIRFLOW_VAR_EMAIL_SEND_TO')
AIRFLOW_VAR_JOB_TYPE = 'SYSTEM'
AIRFLOW_VAR_DAG_ID = "9001_EMAIL_JOB_STATUS_DAILY"
AIRFLOW_VAR_DAG_DESCRIPTION = "TO SEND EMAIL JOB STATUS DAILY PERIODICALLY"
AIRFLOW_VAR_DAG_ALIAS = 'EMAIL_JOB_STATUS_DAILY'
AIRFLOW_VAR_DAG_PATH = 'NONE'

def omt_table():

    bigquery_hook = BigQueryHook(gcp_conn_id='google_cloud_default', delegate_to=None, use_legacy_sql=False,location=AIRFLOW_VAR_LOCATION)


    html_body = ""

    list_status = ['ERROR','RUNNING','DONE']

    for status in list_status:

        row_data = bigquery_hook.get_records(sql="SELECT JOB_TYPE,JOB_NAME,START_DATE,END_DATE,SRC_FILE_OR_TBL_NAME,TRG_TBL_NAME ,STATUS, SPLIT(SUBSTRING(CAST(END_DATE  - START_DATE AS STRING),6),'.')[OFFSET(0)] DURATION from OMT.OMT_PROCESS_LOG WHERE STATUS='"+status+"' AND PRC_DT='"+Variable.get('AIRFLOW_VAR_CURRENT_EXECUTION_DATE')+"' ORDER BY JOB_TYPE,JOB_NAME,START_DATE,TRG_TBL_NAME")
        
        html_body = html_body +  """
        <h2> LIST JOB """+status+ """ </h2>
        <table>
		<thead>
			<tr>
				<th>JOB_TYPE</th>
				<th>JOB_NAME</th>
				<th>START_DATE</th>
                <th>END_DATE</th>
                <th>SRC_FILE_OR_TBL_NAME</th>
				<th>TRG_TBL_NAME</th>
				<th>STATUS</th>
                <th>DURATION</th>
			</tr>
		</thead>
		<tbody>
			"""
        for row in row_data:
            html_body = html_body + "<tr>"+ \
                                "<td>"+str(row[0])+"</td>"+ \
                                "<td>"+str(row[1])+"</td>"+ \
                                "<td>"+str(row[2])+"</td>"+ \
                                "<td>"+str(row[3])+"</td>"+ \
                                "<td>"+str(row[4])+"</td>"+ \
                                "<td>"+str(row[5])+"</td>"+ \
                                "<td>"+str(row[6])+"</td>"+ \
                                "<td>"+str(row[7])+"</td>"+ \
                        "</tr> \n"
        html_body = html_body + """
		    </tbody>
	    </table> """

    html = """
    <!DOCTYPE html>
<html>
<head>
    <title>HTML Table Generator</title> 
    <style>
        table {
            border:1px solid #b3adad;
            border-collapse:collapse;
            padding:5px;
        }
        table th {
            border:1px solid #b3adad;
            padding:5px;
            background: #f0f0f0;
            color: #313030;
        }
        table td {
            border:1px solid #b3adad;
            text-align:center;
            padding:5px;
            background: #ffffff;
            color: #313030;
        }
    </style>
</head>
<body>"""+html_body + """
    
</body>
</html>
    """
    return html

def set_date(**kwargs):
    Variable.set(key="AIRFLOW_VAR_CURRENT_EXECUTION_DATE", value=kwargs['execution_date'])  

with DAG(
    AIRFLOW_VAR_DAG_ID,
    description="Job for Email",
    # schedule="@daily",
    schedule_interval= None,#'01 07 * * *' 
    start_date=datetime(2024, 1, 29, 00, 00, tzinfo=local_tz),
    catchup=False,
) as dag:

    set_execution_date = PythonOperator(
                task_id='set_execution_date',
                python_callable=set_date,
                op_kwargs={'execution_date': "{{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}"}
    )
    send_email = EmailOperator( 
                            task_id='send_email',
                            html_content=omt_table(),
                            to=AIRFLOW_VAR_EMAIL_SEND_TO, 
                            subject="Job Report {{ execution_date.in_timezone('Asia/Jakarta').strftime('%Y-%m-%d') }}",
                            mime_subtype='html', 
                            dag=dag)
    
    set_execution_date >> send_email