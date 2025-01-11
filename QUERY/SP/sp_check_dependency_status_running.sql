CREATE OR REPLACE PROCEDURE `prj-7810ed85d543e33a.sp_dap.sp_check_dependency_status_running`(V_JOB_TYPE STRING, V_JOB_NAME STRING, V_PROJECT_ID STRING, V_DATASET_ID STRING, V_TARGET_TABLE STRING, V_EXECUTION_DATE DATE, V_OMT_JOB_DEPENDENCY_TABLE_NAME STRING, V_OMT_PROCESS_LOG_TABLE_NAME STRING)
BEGIN

  /* CHECK DEPENDENCY STATUS RUNNING berfungsi untuk melakukan pengecekan table table dependency apakah sudah selesai running atau belum. Hal ini berfungsi untuk memeastikan keakuratan data ketika job DAG melakukan proses selanjutnya yaitu Check Dependency CLosing.
  */

  --DECLARE V_JOB_DATE DATE;

  -- SET V_PROJECT_ID= 'explore-358315';
  -- SET V_DATASET_ID= 'DATAMART';	
  -- SET V_TARGET_TABLE= 'PROFIT_AND_LOSS';
  -- SET V_EXECUTION_DATE= '2023-02-22';

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

  /* 1. Cek apakah dari seluruh table dependency , masih ada yang belum jalan, atau masih running untuk prc dt yang sedang di proses*/
  EXECUTE IMMEDIATE '''

  SELECT IF( STATUS is null , "Validation Pass", ERROR("Error : There are still exists job for Dependency table in Scheduled / Running State ("||STATUS||")")) FROM (
  SELECT STRING_AGG(DISTINCT CONCAT(OPL.TRG_TBL_NAME,'- EXECUTION_DATE : ',OPL.EXECUTION_DATE))  STATUS
            FROM (
                    SELECT \'''' || V_EXECUTION_DATE || '''\' LIMIT 1
                ) CALENDAR
              , `''' || V_PROJECT_ID || '`.' || V_OMT_JOB_DEPENDENCY_TABLE_NAME || ''' OD
            LEFT JOIN `''' || V_PROJECT_ID || '`.' || V_OMT_PROCESS_LOG_TABLE_NAME || ''' OPL
            ON 1=1
              AND OPL.TRG_TBL_NAME = OD.SOURCE_TABLE
              AND OPL.EXECUTION_DATE = \'''' || V_EXECUTION_DATE || '''\'
              AND OPL.STATUS <> 'DONE'
            WHERE 1=1  
              AND OD.TARGET_TABLE = \'''' || V_PROJECT_ID || '.' || V_DATASET_ID || '.' || V_TARGET_TABLE || '''\'
              AND OD.ENABLE_FLAG = 'Y'
              )

  ''';

END;