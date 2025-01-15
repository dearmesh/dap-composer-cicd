/**
 * Hanya ada partitions 20230731
 */
SELECT * FROM dm_dap.t_cc_ops_new

/**
 * Hanya ada partitions 20230731 
 */

SELECT crdacct,count(*) FROM dm_dap.t_cc_installment_new
GROUP BY crdacct 
having count(*)=2

SELECT * FROM dm_dap.t_cc_installment_new
WHERE crdacct='240122832364'

CREATE TABLE test.t_cc_ops_new_test
AS
SELECT *  FROM dm_dap.t_cc_ops_new
LIMIT 10

SELECT DISTINCT trx_type  FROM dm_dap.t_cc_installment_new

SELECT * FROM test.t_cc_installment_new_test

SELECT * FROM omt.omt_master_job 
WHERE table_name like '%sum_installment_cc_cust_movement%'

update omt.omt_master_job 
set status_run_execution_date='0'
where 1=1 
AND table_name like '%sum_installment_cc_cust_movement%'

SELECT * FROM omt.omt_process_log 
WHERE 1=1 
AND trg_tbl_name like '%sum_installment_cc_cust_movement%'


SELECT CAST(ecifref AS INT64) ecifref
	   , CAST(acctcust AS INT64) crdacct
	   , 'CC' channel
	   , trx_type trx_type_level_2
--	   , COUNT(*) not_feature
	   , ops_mord_amt 
	   , CAST(CAST(smtttxndte AS STRING) AS DATE FORMAT 'RRRRMMDD') date_new_to_feature
	   , CAST(CAST(smtttxndte AS STRING) AS DATE FORMAT 'RRRRMMDD') Last_trx_date
FROM test.t_cc_installment_new_test

SELECT * 
FROM test.t_cc_ops_new_test


SELECT * FROM test.t_cc_ops_new_test


SELECT CAST(ecifref AS INT64) ecifref
	   , CAST(acctcust AS INT64) acctcust
	   , 'CC' channel
	   , trx_type trx_type_level_2
	   , COUNT(*) not_feature
	   , SUM(ops_mord_amt) ops_mord_amt 
	   , MIN(CAST(CAST(smtttxndte AS STRING) AS DATE FORMAT 'RRRRMMDD')) date_new_to_feature
	   , MAX(CAST(CAST(smtttxndte AS STRING) AS DATE FORMAT 'RRRRMMDD')) Last_trx_date
FROM 
(
SELECT * FROM test.t_cc_installment_new_test
--UNION ALL 
--SELECT * FROM test.t_cc_ops_new_test TO BE CONFIRM TIDAK ADA ACCTCUST, ECIREF, TYPE
)
GROUP BY ecifref,acctcust,trx_type

SELECT * FROM dm_dap.dm_customer_profile_general_new

CREATE TABLE dm_stg_dap.sum_installment_cc_cust_movement AS
SELECT * FROM dm_stg_dap.sum_trx_dcc_cust_movement
LIMIT 0

WITH cte_current_month AS (
          SELECT 
          dcpgn.cif
          , CAST(dctdn.ecif AS INT64) ecif
          , dctdn.acctcust cif_cc
          , 'DCC' channel
          , trx_type_level_2
          , not_feature
          , amt_feature
          , date_new_to_feature
          , Last_trx_date  
      FROM (
		    SELECT CAST(ecifref AS INT64) ecif
			   , CAST(acctcust AS INT64) acctcust
			   , 'CC' channel
			   , trx_type trx_type_level_2
			   , COUNT(*) not_feature
			   , SUM(ops_mord_amt) amt_feature 
			   , MIN(CAST(CAST(smtttxndte AS STRING) AS DATE FORMAT 'RRRRMMDD')) date_new_to_feature
			   , MAX(CAST(CAST(smtttxndte AS STRING) AS DATE FORMAT 'RRRRMMDD')) Last_trx_date
			FROM 
			(
			SELECT * FROM test.t_cc_installment_new_test
			--UNION ALL 
			--SELECT * FROM test.t_cc_ops_new_test TO BE CONFIRM TIDAK ADA ACCTCUST, ECIREF, TYPE
			)
			GROUP BY ecifref,acctcust,trx_type
      ) dctdn
      LEFT JOIN (
              SELECT CAST(cif AS INT64) cif, CAST(ecif AS INT64) AS ecif
              FROM dm_dap.dm_customer_profile_general_new
              WHERE 1=1
              AND DATE_PR = '20230430'
      ) dcpgn ON dctdn.ecif = dcpgn.ecif 
  )
--  , cte_grand_total_current_month AS (
--      --FINANCIAL SUM TOTAL PER CIF
--      SELECT cif
--        ,ecif
--        ,channel
--        ,'Financial' AS trx_type_level_2
--        ,'' flag_transaction
--        ,SUM(not_feature) not_feature
--        ,SUM(amt_feature) amt_feature
--        ,MIN(CAST(date_new_to_feature AS DATE FORMAT 'RRRRMMDD')) date_new_to_feature
--        ,MAX(CAST(last_trx_date AS DATE FORMAT 'RRRRMMDD')) last_trx_date
--      FROM cte_current_month
--      GROUP BY cif,ecif,channel
--      UNION ALL 
--      SELECT 	cif
--            ,ecif
--            ,Channel
--            ,trx_type_level_2
--            ,'' flag_transaction
--            ,not_feature
--            ,amt_feature
--            ,CAST(date_new_to_feature AS DATE FORMAT 'RRRRMMDD') date_new_to_feature
--            ,CAST(Last_trx_date AS DATE FORMAT 'RRRRMMDD') Last_trx_date 
--      FROM cte_current_month
--  )
  , union_current_month_with_last_month AS (
      SELECT 	cif
          ,ecif
          ,cif_cc
          ,Channel
          ,trx_type_level_2
          ,'' flag_transaction
          ,not_feature
          ,amt_feature
          ,date_new_to_feature
          ,Last_trx_date 
      FROM cte_current_month
      UNION ALL
      -- GET LAST MONTH PER CIF
      SELECT 
              cif_funding cif
              ,ecif
              ,cif_cc
              ,Channel
              ,trx_type_level_2
              ,flag_transaction
              ,0 not_feature
              ,0 amt_feature
              ,date_new_to_feature
              ,Last_trx_date 
      FROM dm_stg_dap.sum_installment_cc_cust_movement stdcm
      WHERE 1=1
      AND stdcm.BUSINESS_DATE='2021-12-31' --ambil data sum_trx_dcc di bulan sebelumnya 
  --    AND stdcm.trx_type_level_2 <> 'Financial'
      AND cif_funding||trx_type_level_2 NOT IN (
        SELECT DISTINCT cif||trx_type_level_2 FROM cte_current_month ccm
      )  
  )
  , cte_last_month AS (
    SELECT 
          ucmwlm.cif
          ,ucmwlm.cif_cc
--         ,IFNULL(stdcm.ecif,ucmwlm.ecif) ecif
          ,ucmwlm.ecif
         ,'CC' Channel
         ,ucmwlm.trx_type_level_2
        --  , stdcm.cum_sum_not_feature cum_not_feature_last_month
      /***  JIKA CUM NOT BULAN KEMARIN + NOT THIS MONTH = NOT THIS MONTH = CHURNED
            JIKA CUM NOT BULAN KEMARIN + NOT THIS MONTH > NOT THIS MONTH = STAY
            JIKA CUM NOT BULAN KEMARIN + NOT THIS MONTH > NOT THIS MONTH AND flag_transaction BULAN KEMARIN CHURNED = Reactivate
      ***/ 
         , CASE WHEN stdcm.flag_transaction IS NULL THEN 'New to feature'
              WHEN stdcm.flag_transaction IS NOT NULL AND (IFNULL(stdcm.cum_sum_not_feature,0) + ucmwlm.not_feature) = IFNULL(stdcm.cum_sum_not_feature,0) THEN 'Churned'
              WHEN stdcm.flag_transaction IS NOT NULL AND (IFNULL(stdcm.cum_sum_not_feature,0) + ucmwlm.not_feature) > IFNULL(stdcm.cum_sum_not_feature,0) AND stdcm.flag_transaction <> 'Churned' THEN 'Stay'
              WHEN stdcm.flag_transaction IS NOT NULL AND (IFNULL(stdcm.cum_sum_not_feature,0) + ucmwlm.not_feature) > IFNULL(stdcm.cum_sum_not_feature,0) AND stdcm.flag_transaction = 'Churned' THEN 'Reactivate'
            END flag_transaction  
         , IFNULL(stdcm.date_new_to_feature,ucmwlm.date_new_to_feature) date_new_to_feature --gunakan tanggal bulan sebelumnya, jika baru gunakan tanggal min
         , ucmwlm.Last_trx_date
         , DATE_DIFF('2022-01-31',ucmwlm.Last_trx_date,DAY) Day_diff_last_trx
         , ucmwlm.not_feature
         , ucmwlm.amt_feature
         , IFNULL(stdcm.cum_sum_not_feature,0) + ucmwlm.not_feature cum_sum_not_feature
         , IFNULL(stdcm.cum_amt_feature,0) + ucmwlm.amt_feature cum_amt_feature
--         ,stdcm.flag_transaction
         -- GET ALL NOT MM1 - MM12
         ,stdcm.not_feature not_mm1
         ,stdcm.not_mm1 not_mm2
         ,stdcm.not_mm2 not_mm3
         ,stdcm.not_mm3 not_mm4 
         ,stdcm.not_mm4 not_mm5
         ,stdcm.not_mm5 not_mm6
         ,stdcm.not_mm6 not_mm7
         ,stdcm.not_mm7 not_mm8
         ,stdcm.not_mm8 not_mm9
         ,stdcm.not_mm9 not_mm10
         ,stdcm.not_mm10 not_mm11
         ,stdcm.not_mm11 not_mm12
         ,stdcm.amt_feature amt_mm1
         ,stdcm.amt_mm1 amt_mm2
         ,stdcm.amt_mm2 amt_mm3
         ,stdcm.amt_mm3 amt_mm4 
         ,stdcm.amt_mm4 amt_mm5
         ,stdcm.amt_mm5 amt_mm6
         ,stdcm.amt_mm6 amt_mm7
         ,stdcm.amt_mm7 amt_mm8
         ,stdcm.amt_mm8 amt_mm9
         ,stdcm.amt_mm9 amt_mm10
         ,stdcm.amt_mm10 amt_mm11
         ,stdcm.amt_mm11 amt_mm12
  FROM union_current_month_with_last_month  ucmwlm
  LEFT JOIN (SELECT cif_funding
                        , ecif
                        , cif_cc
                        , channel
                        , trx_type_level_2
                        , cum_sum_not_feature
                        , cum_amt_feature
                        , amt_feature
                        , flag_transaction
                        , date_new_to_feature
                        , not_feature
                        , not_mm1 
                        , not_mm2 
                        , not_mm3 
                        , not_mm4 
                        , not_mm5 
                        , not_mm6 
                        , not_mm7 
                        , not_mm8 
                        , not_mm9 
                        , not_mm10 
                        , not_mm11 
                        , amt_mm1 
                        , amt_mm2 
                        , amt_mm3 
                        , amt_mm4 
                        , amt_mm5 
                        , amt_mm6 
                        , amt_mm7 
                        , amt_mm8 
                        , amt_mm9 
                        , amt_mm10 
                        , amt_mm11 
             FROM 
              dm_stg_dap.sum_installment_cc_cust_movement
              WHERE 1=1
              AND BUSINESS_DATE = '2021-12-31' --ambil data sum_trx_dcc di bulan sebelumnya
            ) stdcm ON ucmwlm.cif = stdcm.cif_funding
              AND ucmwlm.trx_type_level_2 = stdcm.trx_type_level_2
  )
SELECT CAST(cif AS INT64) cif_funding
	  , ecif
	  , cif_cc
	  , trx_type_level_2
	  , CAST(NULL AS STRING) flag_onus
	  , channel
	  , flag_transaction
	  , date_new_to_feature
	  , last_trx_date
	  , day_diff_last_trx
	  , CAST(not_feature AS INT64) not_feature
	  , amt_feature
	  , CAST(cum_sum_not_feature AS INT64) cum_sum_not_feature
	  , cum_amt_feature
	  , not_mm1
	  , not_mm2
	  , not_mm3
	  , not_mm4
	  , not_mm5
	  , not_mm6
	  , not_mm7
	  , not_mm8
	  , not_mm9
	  , not_mm10
	  , not_mm11
	  , not_mm12
	  , amt_mm1
	  , amt_mm2
	  , amt_mm3
	  , amt_mm4
	  , amt_mm5
	  , amt_mm6
	  , amt_mm7
	  , amt_mm8
	  , amt_mm9
	  , amt_mm10
	  , amt_mm11
	  , amt_mm12
      , CAST('2022-12-31' AS DATE FORMAT 'RRRR-MM-DD')
	  , '20221231000000'
	  , CAST('20221231000000' AS DATETIME FORMAT 'RRRRMMDDHH24MISS')
	  , CAST('2022-12-31' AS DATE FORMAT 'RRRR-MM-DD')
FROM cte_last_month