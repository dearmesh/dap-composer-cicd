CREATE TABLE `prj-7810ed85d543e33a.omt.omt_process_log`
(
  job_type STRING(65535),
  job_name STRING(65535),
  start_date DATETIME,
  end_date DATETIME,
  max_last_update_date DATETIME,
  src_file_or_tbl_name STRING(65535),
  trg_temp_file STRING(65535),
  trg_tbl_name STRING(65535),
  delta_data_count INT64,
  src_data_count INT64,
  trg_data_count INT64,
  status STRING(65535),
  status_description STRING(65535),
  execution_date DATE,
  job_id STRING(65535),
  business_date DATE
)
PARTITION BY execution_date
CLUSTER BY job_type, job_name, trg_tbl_name;

CREATE TABLE `prj-7810ed85d543e33a.omt.omt_calendar_date`
(
  DAY_DATE DATETIME,
  DAY_DATE_STRING STRING,
  DAY_NAME STRING,
  DAY_NUM STRING,
  MONTH_NAME STRING,
  MONTH_NUM STRING,
  YEAR_NUM STRING
);

CREATE TABLE `prj-7810ed85d543e33a.omt.omt_error_log`
(
  job_type STRING(65535),
  job_name STRING(65535),
  start_date DATETIME,
  end_date DATETIME,
  src_file_or_tbl_name STRING(65535),
  trg_tbl_name STRING(65535),
  status STRING(65535),
  status_description STRING(65535),
  execution_date DATE,
  job_id STRING(65535),
  business_date DATE
)
PARTITION BY execution_date;

CREATE TABLE `prj-7810ed85d543e33a.omt.omt_job_dependency`
(
  target_table STRING,
  source_table STRING,
  processing_partition_type STRING,
  operand_check_value INT64,
  check_execution_date_flag STRING,
  check_closing_status_flag STRING(1),
  enable_flag STRING(1)
);

CREATE TABLE `prj-7810ed85d543e33a.omt.omt_master_job`
(
  job_name STRING(65535),
  table_name STRING(65535),
  query_parameter STRING(65535),
  execution_date DATE,
  status_run_execution_date STRING(1),
  category STRING(65535),
  enable_flag STRING(1),
  prioritas INT64,
  source_gcs_folder STRING(65535),
  archive_gcs_folder STRING(65535),
  retention_file_day STRING(65535),
  retention_table_day STRING(65535),
  query_create_table STRING(65535),
  target_table_name STRING(65535),
  table_sp_name STRING(65535),
  sts_job_name STRING(65535)
);