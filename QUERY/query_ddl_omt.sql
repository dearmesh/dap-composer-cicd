CREATE TABLE `prj-7810ed85d543e33a.omt.omt_process_delete_log`
(
  job_type STRING,
  job_name STRING,
  start_date DATETIME,
  end_date DATETIME,
  max_last_update_date DATETIME,
  src_file_or_tbl_name STRING,
  trg_temp_file STRING,
  trg_tbl_name STRING,
  delete_row_count INT64,
  status STRING,
  status_description STRING,
  execution_date DATE,
  job_id STRING
)
PARTITION BY execution_date
CLUSTER BY job_type, job_name, trg_tbl_name;

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
  delta_row_count INT64,
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
  day_date DATETIME,
  day_date_string STRING,
  day_name STRING,
  day_num STRING,
  month_name STRING,
  month_num STRING,
  year_num STRING,
  quarter_num STRING,
  semester_num STRING
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

CREATE TABLE `prj-7810ed85d543e33a.omt.omt_bypass_dt_pr_range`
(
  job_type STRING,
  job_name STRING,
  trg_tbl_name STRING,
  enable_flag STRING
);

CREATE TABLE `prj-7810ed85d543e33a.omt.omt_master_job`
(
  job_name STRING,
  table_name STRING,
  query_parameter STRING,
  execution_date DATE,
  status_run_execution_date STRING(1),
  category STRING,
  enable_flag STRING(1),
  prioritas INT64
);