WITH cte_current_month AS (  
  SELECT CAST(sub.cif AS INT64) cif
  	   , CAST(dcpgn.ecif AS INT64) ecif
  	   , sub.channel
  	   , sub.trx_type_level_2
  	   , sub.not_feature
  	   , sub.amt_feature
  	   , sub.date_new_to_feature
  	   , sub.last_trx_date
   FROM (
SELECT CAST(nif.cif AS INT64) cif
	  ,'FX' channel
	  ,nif.ccy_pair trx_type_level_2
	  ,(count(1)) not_feature
	  ,SUM(nif.amount_idr) amt_feature
	  ,MIN(nif.tradedate) date_new_to_feature
	  ,MAX(nif.tradedate) last_trx_date 
  FROM dm_dap.new_investment_fx nif
 WHERE 1=1 
--	  	AND nif.DATE_PR BETWEEN '20230801' AND '20230831' --Ambil data 1 bulan
GROUP BY cif, ccy_pair
  ) sub
  LEFT JOIN 
  (
  	SELECT CAST(cif AS INT64) cif
		 , CAST(ecif AS INT64) ecif 
	FROM dm_dap.dm_customer_profile_general_new
	WHERE 1=1
--		AND date_pr='20230430'
  ) dcpgn 
  ON 1=1
  	AND sub.cif = dcpgn.cif 
  WHERE 1=1
  )
-- select * from cte_current_month   
, cte_grand_total_current_month AS (
  --FINANCIAL SUM TOTAL PER CIF
	SELECT 
	  	cif
	    , ecif
	    , channel
	    , trx_type_level_2
	    , '' flag_transaction
	    , SUM(not_feature) not_feature
	    , SUM(amt_feature) amt_feature
	    , MIN(CAST(date_new_to_feature AS DATE)) date_new_to_feature
	    , MAX(CAST(last_trx_date AS DATE)) last_trx_date
	  FROM cte_current_month
	  GROUP BY cif,ecif,channel,trx_type_level_2
	  UNION ALL 
	  SELECT 
	  	cif
	    , ecif
	    , Channel
	    , trx_type_level_2
	    , '' flag_transaction
	    , not_feature
	    , amt_feature
	    , CAST(date_new_to_feature AS DATE) date_new_to_feature
	    , CAST(last_trx_date AS DATE) Last_trx_date 
	  FROM cte_current_month
)
--select * from cte_grand_total_current_month
, union_current_month_with_last_month AS (
  SELECT cif
        ,ecif
        ,Channel
        ,trx_type_level_2
        ,'' flag_transaction
        ,not_feature
        ,amt_feature
        ,date_new_to_feature
        ,Last_trx_date 
    FROM cte_grand_total_current_month
  UNION ALL
  -- GET LAST MONTH PER CIF
  SELECT cif_funding cif
        ,ecif
        ,Channel
        ,trx_type_level_2
        ,flag_transaction
        ,0 not_feature
        ,0 amt_feature
        ,date_new_to_feature
        ,Last_trx_date 
    FROM dm_stg_dap.sum_trx_fx_cust_movement stfcm
   WHERE 1=1
    --  AND stfcm.business_date = '2021-12-31' --ambil data sum_trx_dbankpro di bulan sebelumnya 
    --  AND stfcm.business_date = DATE_SUB(CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE), INTERVAL 1 MONTH) --ambil data sum_trx_dbankpro di bulan sebelumnya 
     AND cif_funding||trx_type_level_2 NOT IN ( SELECT DISTINCT cif||trx_type_level_2 FROM cte_grand_total_current_month ccm )  
  )
--  select * from union_current_month_with_last_month
, cte_last_month AS (
  SELECT ucmwlm.cif
        ,ucmwlm.ecif
        ,ucmwlm.channel
        ,ucmwlm.trx_type_level_2
          --  , stfcm.cum_sum_not_feature cum_not_feature_last_month
        ,CASE WHEN stfcm.flag_transaction IS NULL THEN 'New to feature'
            WHEN stfcm.flag_transaction IS NOT NULL AND (IFNULL(stfcm.cum_sum_not_feature,0) + ucmwlm.not_feature) = IFNULL(stfcm.cum_sum_not_feature,0) THEN 'Churned'
            WHEN stfcm.flag_transaction IS NOT NULL AND (IFNULL(stfcm.cum_sum_not_feature,0) + ucmwlm.not_feature) > IFNULL(stfcm.cum_sum_not_feature,0) AND stfcm.flag_transaction <> 'Churned' THEN 'Stay'
            WHEN stfcm.flag_transaction IS NOT NULL AND (IFNULL(stfcm.cum_sum_not_feature,0) + ucmwlm.not_feature) > IFNULL(stfcm.cum_sum_not_feature,0) AND stfcm.flag_transaction = 'Churned' THEN 'Reactivate'
          END flag_transaction  
        ,IFNULL(stfcm.date_new_to_feature,ucmwlm.date_new_to_feature) date_new_to_feature --gunakan tanggal bulan sebelumnya, jika baru gunakan tanggal min
        ,ucmwlm.Last_trx_date
        ,DATE_DIFF('2022-01-31',ucmwlm.Last_trx_date,DAY) Day_diff_last_trx
        ,ucmwlm.not_feature
        ,ucmwlm.amt_feature
        ,IFNULL(stfcm.cum_sum_not_feature,0) + ucmwlm.not_feature cum_sum_not_feature
        ,IFNULL(stfcm.cum_amt_feature,0) + ucmwlm.amt_feature cum_amt_feature
  --------------- GET ALL NOT MM1 - MM12 --------------- 
        ,stfcm.not_feature not_mm1
        ,stfcm.not_mm1 not_mm2
        ,stfcm.not_mm2 not_mm3
        ,stfcm.not_mm3 not_mm4 
        ,stfcm.not_mm4 not_mm5
        ,stfcm.not_mm5 not_mm6
        ,stfcm.not_mm6 not_mm7
        ,stfcm.not_mm7 not_mm8
        ,stfcm.not_mm8 not_mm9
        ,stfcm.not_mm9 not_mm10
        ,stfcm.not_mm10 not_mm11
        ,stfcm.not_mm11 not_mm12
        ,stfcm.amt_feature amt_mm1
        ,stfcm.amt_mm1 amt_mm2
        ,stfcm.amt_mm2 amt_mm3
        ,stfcm.amt_mm3 amt_mm4 
        ,stfcm.amt_mm4 amt_mm5
        ,stfcm.amt_mm5 amt_mm6
        ,stfcm.amt_mm6 amt_mm7
        ,stfcm.amt_mm7 amt_mm8
        ,stfcm.amt_mm8 amt_mm9
        ,stfcm.amt_mm9 amt_mm10
        ,stfcm.amt_mm10 amt_mm11
        ,stfcm.amt_mm11 amt_mm12
    FROM union_current_month_with_last_month  ucmwlm
LEFT JOIN (SELECT cif_funding
                 ,ecif
                 ,cif_cc
                 ,channel
                 ,trx_type_level_2
                 ,cum_sum_not_feature
                 ,cum_amt_feature
                 ,amt_feature
                 ,flag_transaction
                 ,date_new_to_feature
                 ,not_feature
                 ,not_mm1 
                 ,not_mm2 
                 ,not_mm3 
                 ,not_mm4 
                 ,not_mm5 
                 ,not_mm6 
                 ,not_mm7 
                 ,not_mm8 
                 ,not_mm9 
                 ,not_mm10 
                 ,not_mm11 
                 ,amt_mm1 
                 ,amt_mm2 
                 ,amt_mm3 
                 ,amt_mm4 
                 ,amt_mm5 
                 ,amt_mm6 
                 ,amt_mm7 
                 ,amt_mm8 
                 ,amt_mm9 
                 ,amt_mm10 
                 ,amt_mm11 
             FROM dm_stg_dap.sum_trx_fx_cust_movement
            WHERE 1=1
                -- AND business_date = '2021-12-31' --ambil data sum_trx_dbankpro di bulan sebelumnya
              -- AND business_date = DATE_SUB(CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE), INTERVAL 1 MONTH) --ambil data sum_trx_dbankpro di bulan sebelumnya 
              ) stfcm ON ucmwlm.cif = stfcm.cif_funding
                     AND ucmwlm.trx_type_level_2 = stfcm.trx_type_level_2
  )
--select * from cte_last_month
  SELECT CAST(cif AS INT64) cif_funding
  	    ,ecif
  	    ,NULL cif_cc
  	    ,trx_type_level_2
  	    ,CAST(NULL AS STRING) flag_onus
  	    ,channel
  	    ,flag_transaction
  	    ,date_new_to_feature
  	    ,last_trx_date
  	    ,day_diff_last_trx
  	    ,CAST(not_feature AS INT64) not_feature
  	    ,amt_feature
  	    ,CAST(cum_sum_not_feature AS INT64) cum_sum_not_feature
  	    ,cum_amt_feature
  	    ,not_mm1
  	    ,not_mm2
  	    ,not_mm3
  	    ,not_mm4
  	    ,not_mm5
  	    ,not_mm6
  	    ,not_mm7
  	    ,not_mm8
  	    ,not_mm9
  	    ,not_mm10
  	    ,not_mm11
  	    ,not_mm12
  	    ,amt_mm1
  	    ,amt_mm2
  	    ,amt_mm3
  	    ,amt_mm4
  	    ,amt_mm5
  	    ,amt_mm6
  	    ,amt_mm7
  	    ,amt_mm8
  	    ,amt_mm9
  	    ,amt_mm10
  	    ,amt_mm11
  	    ,amt_mm12
--        ,CAST(\'''' || cursor.EXECUTION_DATE || '''\' AS DATE) EXECUTION_DATE
--        ,\'''' || V_JOB_ID || '''\' JOB_ID
--        ,PARSE_DATETIME("%Y%m%d%H%M%S", \'''' || V_JOB_ID || '''\') JOB_ID_DATE_FORMAT
--        ,CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE) BUSINESS_DATE
    FROM cte_last_month