from datetime import datetime, timedelta, time
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataform import DataformCreateCompilationResultOperator,DataformCreateWorkflowInvocationOperator,DataformQueryWorkflowInvocationActionsOperator,DataformGetCompilationResultOperator
import pendulum

local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'start_date': datetime(2000, 1, 1, 1, 1, tzinfo=local_tz)
    , 'retries': 0
}

PROJECT_ID = "prj-7810ed85d543e33a"
REPOSITORY_ID = "dataform-processing-data-dap"
REGION = "asia-southeast2"
WORKSPACE_ID = "dw_dap"


compilation_result = {
    "workspace" : "projects/prj-7810ed85d543e33a/locations/asia-southeast2/repositories/dataform-processing-data-dap/workspaces/dw_dap",
    "code_compilation_config" : {
    "vars": {
        "table_a_columns": "test123",
        "ini_variable": "test321"
    }
    }
}

target = {
  "database": "prj-7810ed85d543e33a",
  "schema": "dw_dap",
  "name": "table_c"
}

with DAG('dataform_test'
    , default_args=default_args
    , description='test desc'
    , schedule_interval=None
    , catchup=False
    , tags=["DATA_FORM"]) as AIRFLOW_VAR_DAP_DAG_ALIAS:

    Start = EmptyOperator(task_id="Start")

    create_compilation_task = DataformCreateCompilationResultOperator(
    task_id='create_dataform_compilation',
    project_id=PROJECT_ID,
    region=REGION,
    repository_id=REPOSITORY_ID,
    compilation_result=compilation_result
    )

    get_compilation_result = DataformGetCompilationResultOperator(
        task_id="get-compilation-result",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result_id=(
            "{{ task_instance.xcom_pull('create_dataform_compilation')['name'].split('/')[-1] }}"
        ),
)

    create_workflow_invoke = DataformCreateWorkflowInvocationOperator(
        task_id='workflow_invoke',
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result" : "{{ task_instance.xcom_pull('create_dataform_compilation')['name'] }}",
            'invocation_config': {"included_targets": [target]}

        }
    )

    query_workflow_invocation_actions = DataformQueryWorkflowInvocationActionsOperator(
        task_id="query-workflow-invocation-actions",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation_id=("{{ task_instance.xcom_pull('workflow_invoke')['name'].split('/')[-1] }}"
    ),
)

    Start >> create_compilation_task >> get_compilation_result >> create_workflow_invoke >> query_workflow_invocation_actions