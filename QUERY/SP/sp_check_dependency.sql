-- CREATE OR REPLACE PROCEDURE `prj-7810ed85d543e33a.sp_dap.sp_check_dependency`(V_JOB_TYPE STRING, V_JOB_NAME STRING, V_PROJECT_ID STRING, V_DATASET_ID STRING, V_TARGET_TABLE STRING, V_EXECUTION_DATE DATE, V_PROCESS_FLAG STRING, V_OMT_JOB_DEPENDENCY_TABLE_NAME STRING, V_OMT_PROCESS_LOG_TABLE_NAME STRING, V_AIRFLOW_VAR_DAP_DATASET_TEMP_SP STRING)
CREATE OR REPLACE PROCEDURE `sp_dap.sp_check_dependency`(V_JOB_TYPE STRING, V_JOB_NAME STRING, V_PROJECT_ID STRING, V_DATASET_ID STRING, V_TARGET_TABLE STRING, V_EXECUTION_DATE DATE, V_PROCESS_FLAG STRING, V_OMT_JOB_DEPENDENCY_TABLE_NAME STRING, V_OMT_PROCESS_LOG_TABLE_NAME STRING, V_AIRFLOW_VAR_DAP_DATASET_TEMP_SP STRING)
BEGIN

  /* CHECK DEPENDENCY berfungsi untuk melakukan check dependency di table omt_process_log. Jika ada yang belum ready, maka akan diberikan flag dan dalam cek dependency ini akan dilakukan return untuk status error di Flow DAG. sehingga statusnya bisa jadi error ataupun success.  Jika error maka akan dilakukan retry hinggal limit waktu yang ditentukan
  */

  DECLARE V_TABLE_CLOSING_STATUS STRING;
  --DECLARE V_JOB_DATE DATE;

  -- SET V_PROJECT_ID= 'explore-358315';
  -- SET V_DATASET_ID= 'DATAMART';	
  -- SET V_TARGET_TABLE= 'PROFIT_AND_LOSS';
  -- SET V_EXECUTION_DATE= '2023-02-22';

  --CREATE TABLE temp_sp_dap DEPENDENCY
  EXECUTE IMMEDIATE '''
  CREATE OR REPLACE TABLE `''' || V_PROJECT_ID || '`.' || V_AIRFLOW_VAR_DAP_DATASET_TEMP_SP || '.' || V_TARGET_TABLE || '''_business_date_list
    (PROCESS_BUSINESS_DATE DATE
    , EXECUTION_DATE DATE
    , ATTRIBUTE1 STRING
    , ATTRIBUTE2 STRING
    , ATTRIBUTE3 STRING)
  ''';

  --CREATE TABLE temp_sp_dap DEPENDENCY
  EXECUTE IMMEDIATE '''
  CREATE OR REPLACE TABLE `''' || V_PROJECT_ID || '`.' || V_AIRFLOW_VAR_DAP_DATASET_TEMP_SP || '.' || V_TARGET_TABLE || '''_dependency
    ( EXECUTION_DATE DATE
      , PROCESS_BUSINESS_DATE DATE
      , CHECK_BUSINESS_DATE_DEPENDENCY DATE
      , SOURCE_TABLE STRING 
      , CHECK_EXECUTION_DATE_FLAG STRING
      , OPERAND_CHECK_VALUE INT64
      , CHECK_CLOSING_STATUS_FLAG STRING
      , ENABLE_FLAG STRING
      , STATUS STRING
      , ATTRIBUTE1 STRING
      , ATTRIBUTE2 STRING
      , ATTRIBUTE3 STRING
    )
  ''';

  /*Pre Check , setiap table dm_stg atau dm wajib sudah ada di table OMT.DEPENDENCY*/
  EXECUTE IMMEDIATE '''
  SELECT 
      IF(
          IFNULL(SUB.count_error,0) <> 0
          , 'PASS'
          , ERROR('Error : Please Register the Table in OMT DEPENDENCY First for Validation')
      )
      FROM
      (
          SELECT count(1) count_error 
          FROM `''' || V_PROJECT_ID || '`.' || V_OMT_JOB_DEPENDENCY_TABLE_NAME || ''' OD
          WHERE 1=1
            AND OD.TARGET_TABLE = \'''' || V_PROJECT_ID || '.' || V_DATASET_ID || '.' || V_TARGET_TABLE || '''\'
      ) SUB
      WHERE 1=1
  ''';


  -- /* 1. DAPATKAN BUSINESS_DATE DISTINCT yang ada di OMT_PROCESS_LOG dan OMT_DEPENDENCY*/

    EXECUTE IMMEDIATE '''
    INSERT `''' || V_PROJECT_ID || '`.' || V_AIRFLOW_VAR_DAP_DATASET_TEMP_SP || '.' || V_TARGET_TABLE || '''_business_date_list
    /*Untuk mengisi ada BUSINESS_DATE yang di cek dan dicoba untuk di proses (Proses Normal Daily H -1)*/
    SELECT CAST(DATE_SUB(\'''' || V_EXECUTION_DATE || '''\' , INTERVAL 1 DAY) AS DATE ) PROCESS_BUSINESS_DATE
      , CAST(\'''' || V_EXECUTION_DATE || '''\' AS DATE) EXECUTION_DATE
      , 'REQUIRED DAILY' ATTRIBUTE1
      , CAST(NULL AS STRING) ATTRIBUTE2
      , CAST(NULL AS STRING) ATTRIBUTE3
    FROM (SELECT 1 LIMIT 1)
    WHERE 1=1
      AND \'''' || V_PROCESS_FLAG || '''\' = 'DAILY'
    UNION DISTINCT 
    /*Untuk mengisi ada BUSINESS_DATE yang di cek dan dicoba untuk di proses (Proses Normal Daily M-1)*/
    SELECT CAST(LAST_DAY(DATE_SUB(\'''' || V_EXECUTION_DATE || '''\' , INTERVAL 1 MONTH) ) AS DATE) PROCESS_BUSINESS_DATE
      , CAST(\'''' || V_EXECUTION_DATE || '''\' AS DATE) EXECUTION_DATE
      , 'REQUIRED MONTHLY' ATTRIBUTE1
      , CAST(NULL AS STRING) ATTRIBUTE2
      , CAST(NULL AS STRING) ATTRIBUTE3
    FROM (SELECT 1 LIMIT 1)
    WHERE 1=1
      AND \'''' || V_PROCESS_FLAG || '''\' = 'MONTHLY'
    ''';

  IF (V_PROCESS_FLAG = 'MONTHLY') THEN

    EXECUTE IMMEDIATE '''
    UPDATE `''' || V_PROJECT_ID || '`.' || V_AIRFLOW_VAR_DAP_DATASET_TEMP_SP || '.' || V_TARGET_TABLE || '''_business_date_list
    SET PROCESS_BUSINESS_DATE = LAST_DAY('2022-01-31')
    WHERE 1=1
      AND PROCESS_BUSINESS_DATE < ('2022-01-31')
    ''';

  ELSEIF (V_PROCESS_FLAG = 'DAILY') THEN 

    EXECUTE IMMEDIATE '''
    UPDATE `''' || V_PROJECT_ID || '`.' || V_AIRFLOW_VAR_DAP_DATASET_TEMP_SP || '.' || V_TARGET_TABLE || '''_business_date_list
    SET PROCESS_BUSINESS_DATE = '2022-01-01'
    WHERE 1=1
      AND PROCESS_BUSINESS_DATE < ('2022-01-01')
    ''';

  END IF;

  -- /*2. Insert data untuk mendapatkan status readiness table source berdasarkan aturan check_business_date*/
  EXECUTE IMMEDIATE '''
  INSERT `''' || V_PROJECT_ID || '`.' || V_AIRFLOW_VAR_DAP_DATASET_TEMP_SP || '.' || V_TARGET_TABLE || '''_dependency
  SELECT SUB2.EXECUTION_DATE
    , SUB2.PROCESS_BUSINESS_DATE
    , SUB2.CHECK_BUSINESS_DATE_DEPENDENCY
    , SUB2.SOURCE_TABLE
    , SUB2.CHECK_EXECUTION_DATE_FLAG
    , SUB2.OPERAND_CHECK_VALUE
    , SUB2.CHECK_CLOSING_STATUS_FLAG
    , SUB2.ENABLE_FLAG
    , IFNULL(SUB2.STATUS,'NOT_READY')
    , SUB2.ATTRIBUTE1 
    , SUB2.ATTRIBUTE2 
    , SUB2.ATTRIBUTE3
  FROM
    (
      SELECT DISTINCT 
          SUB1.EXECUTION_DATE
          , SUB1.PROCESS_BUSINESS_DATE
          , SUB1.CHECK_BUSINESS_DATE_DEPENDENCY
          , SUB1.SOURCE_TABLE
          , SUB1.CHECK_EXECUTION_DATE_FLAG
          , SUB1.OPERAND_CHECK_VALUE
          , SUB1.CHECK_CLOSING_STATUS_FLAG
          , SUB1.ENABLE_FLAG
          , IFNULL(IF(SUB1.ENABLE_FLAG = 'N', 'PASS', IF(OPL.TRG_TBL_NAME is NULL,'NOT_READY','READY')),'NOT_READY') STATUS
          , SUB1.ATTRIBUTE1 
          , SUB1.ATTRIBUTE2 
          , SUB1.ATTRIBUTE3
      FROM 
        (
          SELECT DISTINCT 
              DPD.EXECUTION_DATE
              , DPD.PROCESS_BUSINESS_DATE
              , CASE WHEN OD.PROCESSING_PARTITION_TYPE = 'MONTHLY' THEN  LAST_DAY(DATE_SUB(DPD.PROCESS_BUSINESS_DATE , INTERVAL ABS(OD.OPERAND_CHECK_VALUE) MONTH))
                     WHEN OD.PROCESSING_PARTITION_TYPE = 'NON_PARTITION' THEN DPD.PROCESS_BUSINESS_DATE - ABS(OD.OPERAND_CHECK_VALUE+0)
                     ELSE DPD.PROCESS_BUSINESS_DATE - ABS(CASE WHEN OD.OPERAND_CHECK_VALUE = 0 THEN OD.OPERAND_CHECK_VALUE+0
                                                            ELSE OD.OPERAND_CHECK_VALUE + 1
                                                          END) 
                END CHECK_BUSINESS_DATE_DEPENDENCY
              , OD.SOURCE_TABLE SOURCE_TABLE
              , OD.CHECK_EXECUTION_DATE_FLAG
              , OD.OPERAND_CHECK_VALUE
              , OD.CHECK_CLOSING_STATUS_FLAG
              , OD.ENABLE_FLAG
              , DPD.ATTRIBUTE1 
              , DPD.ATTRIBUTE2 
              , DPD.ATTRIBUTE3 
          FROM `''' || V_PROJECT_ID || '`.' || V_AIRFLOW_VAR_DAP_DATASET_TEMP_SP || '.' || V_TARGET_TABLE || '''_business_date_list DPD
            , `''' || V_PROJECT_ID || '`.' || V_OMT_JOB_DEPENDENCY_TABLE_NAME || ''' OD
          WHERE 1=1
            AND OD.TARGET_TABLE = \'''' || V_PROJECT_ID || '.' || V_DATASET_ID || '.' || V_TARGET_TABLE || '''\'
            AND DPD.EXECUTION_DATE = \'''' || V_EXECUTION_DATE || '''\'
            AND OD.ENABLE_FLAG = 'Y'
        ) SUB1
      LEFT JOIN ( SELECT SUB.*
                  FROM
                  (
                    SELECT JOB_TYPE
                        , JOB_NAME
                        , TRG_TBL_NAME 
                        , EXECUTION_DATE 
                        , BUSINESS_DATE 
                        , LAG(EXECUTION_DATE) OVER (PARTITION BY JOB_TYPE, JOB_NAME, TRG_TBL_NAME ORDER BY BUSINESS_DATE DESC) FLAG_MAX_EXECUTION_DATE_MASTER
                    FROM `''' || V_PROJECT_ID || '`.' || V_OMT_PROCESS_LOG_TABLE_NAME || '''
                    WHERE 1=1
                      AND EXECUTION_DATE between DATE_SUB(\'''' || V_EXECUTION_DATE || '''\' , INTERVAL 36 MONTH) and DATE_ADD(\'''' || V_EXECUTION_DATE || '''\' , INTERVAL 3 MONTH)
                      AND STATUS = 'DONE'
                      -- AND IFNULL(STATUS_DESCRIPTION,'') <> 'JOB_STATUS_FLAG'
                      AND TRG_TBL_NAME||EXECUTION_DATE NOT IN (
                                                          SELECT DISTINCT TRG_TBL_NAME||EXECUTION_DATE
                                                          FROM `''' || V_PROJECT_ID || '`.' || V_OMT_PROCESS_LOG_TABLE_NAME || '''
                                                          WHERE 1=1
                                                            AND EXECUTION_DATE between DATE_SUB(\'''' || V_EXECUTION_DATE || '''\' , INTERVAL 36 MONTH) and DATE_ADD(\'''' || V_EXECUTION_DATE || '''\' , INTERVAL 3 MONTH)
                                                            AND STATUS in ('ERROR','RUNNING')
                                                            AND IFNULL(STATUS_DESCRIPTION,'') <> 'JOB_STATUS_FLAG'
                                                      )
                  ) SUB
                  WHERE 1=1
                    AND IF(SUB.JOB_TYPE like '%MASTER%',SUB.FLAG_MAX_EXECUTION_DATE_MASTER is NULL, 1=1)
                ) OPL
        ON SUB1.SOURCE_TABLE = OPL.TRG_TBL_NAME
          AND IF(OPL.JOB_TYPE like '%MASTER%', SUB1.EXECUTION_DATE <= OPL.EXECUTION_DATE 
				          ,IF( SUB1.CHECK_EXECUTION_DATE_FLAG = 'Y' ,SUB1.EXECUTION_DATE = OPL.EXECUTION_DATE 
					            ,SUB1.CHECK_BUSINESS_DATE_DEPENDENCY = OPL.BUSINESS_DATE)
			  )
      WHERE 1=1
      ORDER BY SUB1.PROCESS_BUSINESS_DATE
    ) SUB2
  WHERE 1=1
  ''';

  /* 3. Cek apakah dari seluruh row ada yang NOT_READY*/
  EXECUTE IMMEDIATE '''

  SELECT IF( STATUS is null , "Validation Pass", ERROR("Error : There are still exists Dependency table with status Not Ready or Not Pass ("||STATUS||")")) FROM (
  SELECT STRING_AGG(DISTINCT CONCAT(OPL.SOURCE_TABLE,'- EXECUTION_DATE : ',OPL.EXECUTION_DATE))  STATUS
  FROM `''' || V_PROJECT_ID || '`.' || V_AIRFLOW_VAR_DAP_DATASET_TEMP_SP || '.' || V_TARGET_TABLE || '''_dependency OPL
  WHERE 1=1
    AND (STATUS like ('%NOT_READY%') 
        OR STATUS like ('%NOT_PASS%')
        )
    )
  ''';

END;