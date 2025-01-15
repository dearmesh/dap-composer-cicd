-- CREATE OR REPLACE PROCEDURE `prj-7810ed85d543e33a.sp_dap.sp_validate_master_job`(V_JOB_NAME STRING, V_PROJECT_ID STRING, V_DATASET_ID STRING, V_TARGET_TABLE STRING, V_CATEGORY_JOB STRING)
CREATE OR REPLACE PROCEDURE `sp_dap.sp_validate_master_job`(V_JOB_NAME STRING, V_PROJECT_ID STRING, V_DATASET_ID STRING, V_TARGET_TABLE STRING, V_CATEGORY_JOB STRING)
BEGIN

  DECLARE row_count INT64;

  -- -- 1. Cek apakah ada data di tabel
  -- SET row_count = (
  --   SELECT COUNT(*)
  --   FROM omt.omt_master_job OMJ
  --   WHERE job_name = V_JOB_NAME AND table_name = CONCAT(V_PROJECT_ID, ".", V_DATASET_ID, ".", V_TARGET_TABLE)
  -- );

  -- -- 2. Jika ada data, return 0
  -- IF row_count > 0 THEN
  --   SELECT 1;

  -- -- Jika tidak ada data, lanjutkan untuk insert data
  -- ELSE
  --   -- Logika insert data Anda di sini.  Ganti pernyataan di bawah ini dengan pernyataan INSERT Anda.
  --   INSERT INTO omt.omt_master_job 
  --     ( job_name
  --       , table_name
  --       , query_parameter
  --       , execution_date
  --       , status_run_execution_date
  --       , category
  --       , enable_flag
  --       , prioritas
  --     )
  --   values
  --     (
  --       V_JOB_NAME
  --       , V_PROJECT_ID || '.' || V_DATASET_ID || '.' || V_TARGET_TABLE
  --       , NULL
  --       , CURRENT_DATE('Asia/Jakarta')
  --       , '0'
  --       , V_CATEGORY_JOB
  --       , '1'
  --       , NULL
  --     );
  -- END IF;


  /*Pre Check , setiap table DATAMART wajib sudah ada di table OMT.DEPENDENCY*/
  EXECUTE IMMEDIATE '''
  SELECT 
      IF(
          IFNULL(SUB.count_error,0) <> 0
          , 'PASS'
          , ERROR('Error : Please Register Table in OMT MASTER JOB with all the parameter required First for Validation')
      )
      FROM
      (
          SELECT count(1) count_error 
          FROM omt.omt_master_job OMJ
          WHERE 1=1
            AND OMJ.table_name = \'''' || V_PROJECT_ID || '.' || V_DATASET_ID || '.' || V_TARGET_TABLE || '''\'
            AND OMJ.job_name = \'''' || V_JOB_NAME || '''\'
            -- AND OMJ.enable_flag='1' 
      ) SUB
      WHERE 1=1
  ''';

  /*Pre Check , jika execution date kolom belum sama dengan Current Date , maka akan diupdate ke Current date dan kolom
  status_run_execution_date akan diupdate menjadi 0*/
  IF EXISTS (
    SELECT 1
    FROM omt.omt_master_job OMJ
    WHERE 1=1
      AND job_name = V_JOB_NAME 
      AND table_name = CONCAT(V_PROJECT_ID, ".", V_DATASET_ID, ".", V_TARGET_TABLE)
      AND execution_date != CURRENT_DATE()
  ) THEN
    -- Update kolom jika tidak sesuai dengan current date
    UPDATE omt.omt_master_job OMJ
    SET execution_date = CURRENT_DATE()
      , status_run_execution_date = '0'
    WHERE 1=1
      AND job_name = V_JOB_NAME 
      AND table_name = CONCAT(V_PROJECT_ID, ".", V_DATASET_ID, ".", V_TARGET_TABLE)
      AND execution_date != CURRENT_DATE();
  END IF;


END;