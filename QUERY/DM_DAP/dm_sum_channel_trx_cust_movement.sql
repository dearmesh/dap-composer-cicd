WITH current_month AS (
	SELECT cif_funding
		   , ecif
		   , cif_cc
		   , trx_type_level_2
		   , channel
		   , flag_transaction
		   , date_new_to_feature
		   , last_trx_date
		   , day_diff_last_trx
		   , not_feature
		   , amt_feature
		   , cum_sum_not_feature
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
	FROM dm_stg_dap.sum_trx_atmcdmcrm_cust_movement
	WHERE 1=1
	AND business_date = '2022-01-31'
	AND trx_type_level_2 <> 'Financial'
	UNION ALL
	SELECT cif_funding
		   , ecif
		   , cif_cc
		   , trx_type_level_2
		   , channel
		   , flag_transaction
		   , date_new_to_feature
		   , last_trx_date
		   , day_diff_last_trx
		   , not_feature
		   , amt_feature
		   , cum_sum_not_feature
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
	FROM dm_stg_dap.sum_trx_branch_cust_movement
	WHERE 1=1
	AND business_date = '2022-01-31'
	AND trx_type_level_2 <> 'Financial'
	UNION ALL
	SELECT cif_funding
		   , ecif
		   , cif_cc
		   , trx_type_level_2
		   , channel
		   , flag_transaction
		   , date_new_to_feature
		   , last_trx_date
		   , day_diff_last_trx
		   , not_feature
		   , amt_feature
		   , cum_sum_not_feature
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
	FROM dm_stg_dap.sum_trx_cc_cust_movement
	WHERE 1=1
	AND business_date = '2022-01-31'
	AND trx_type_level_2 <> 'Financial'
	UNION ALL
	SELECT cif_funding
		   , ecif
		   , cif_cc
		   , trx_type_level_2
		   , channel
		   , flag_transaction
		   , date_new_to_feature
		   , last_trx_date
		   , day_diff_last_trx
		   , not_feature
		   , amt_feature
		   , cum_sum_not_feature
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
	FROM dm_stg_dap.sum_trx_dbankpro_cust_movement
	WHERE 1=1
	AND business_date = '2022-01-31'
	AND trx_type_level_2 <> 'Financial'
	UNION ALL
	SELECT cif_funding
		   , ecif
		   , cif_cc
		   , trx_type_level_2
		   , channel
		   , flag_transaction
		   , date_new_to_feature
		   , last_trx_date
		   , day_diff_last_trx
		   , not_feature
		   , amt_feature
		   , cum_sum_not_feature
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
	FROM dm_stg_dap.sum_trx_dc_cust_movement
	WHERE 1=1
	AND business_date = '2022-01-31'
	AND trx_type_level_2 <> 'Financial'
	UNION ALL
	SELECT cif_funding
		   , ecif
		   , cif_cc
		   , trx_type_level_2
		   , channel
		   , flag_transaction
		   , date_new_to_feature
		   , last_trx_date
		   , day_diff_last_trx
		   , not_feature
		   , amt_feature
		   , cum_sum_not_feature
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
	FROM dm_stg_dap.sum_trx_dcc_cust_movement
	WHERE 1=1
	AND business_date = '2022-01-31'
	AND trx_type_level_2 <> 'Financial'
	UNION ALL
	SELECT cif_funding
		   , ecif
		   , cif_cc
		   , trx_type_level_2
		   , channel
		   , flag_transaction
		   , date_new_to_feature
		   , last_trx_date
		   , day_diff_last_trx
		   , not_feature
		   , amt_feature
		   , cum_sum_not_feature
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
	FROM dm_stg_dap.sum_trx_directdebit_cust_movement
	WHERE 1=1
	AND business_date = '2022-01-31'
	AND trx_type_level_2 <> 'Financial'
	UNION ALL
	SELECT cif_funding
		   , ecif
		   , cif_cc
		   , trx_type_level_2
		   , channel
		   , flag_transaction
		   , date_new_to_feature
		   , last_trx_date
		   , day_diff_last_trx
		   , not_feature
		   , amt_feature
		   , cum_sum_not_feature
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
	FROM dm_stg_dap.sum_trx_si_cust_movement
	WHERE 1=1
	AND business_date = '2022-01-31'
	AND trx_type_level_2 <> 'Financial'
)
, summary_current_month AS (
	SELECT cif_funding
	   , ecif
	   , cif_cc
	   , channel
	   , '' flag_transaction
	   , MIN(date_new_to_feature) date_new_to_feature
	   , MAX(last_trx_date) last_trx_date
--	   , MIN(day_diff_last_trx) day_diff_last_trx
	   , DATE_DIFF('2022-01-31',MIN(last_trx_date),DAY) Day_diff_last_trx
	   , SUM(not_feature) not_feature
	   , SUM(amt_feature) amt_feature
	   , SUM(cum_sum_not_feature) cum_sum_not_feature
	   , SUM(cum_amt_feature) cum_amt_feature
	   , SUM(not_mm1) not_mm1
	   , SUM(not_mm2) not_mm2
	   , SUM(not_mm3) not_mm3
	   , SUM(not_mm4) not_mm4
	   , SUM(not_mm5) not_mm5
	   , SUM(not_mm6) not_mm6
	   , SUM(not_mm7) not_mm7
	   , SUM(not_mm8) not_mm8
	   , SUM(not_mm9) not_mm9
	   , SUM(not_mm10) not_mm10
	   , SUM(not_mm11) not_mm11
	   , SUM(not_mm12) not_mm12
	   , SUM(amt_mm1) amt_mm1
	   , SUM(amt_mm2) amt_mm2
	   , SUM(amt_mm3) amt_mm3
	   , SUM(amt_mm4) amt_mm4
	   , SUM(amt_mm5) amt_mm5
	   , SUM(amt_mm6) amt_mm6
	   , SUM(amt_mm7) amt_mm7
	   , SUM(amt_mm8) amt_mm8
	   , SUM(amt_mm9) amt_mm9
	   , SUM(amt_mm10) amt_mm10
	   , SUM(amt_mm11) amt_mm11
	   , SUM(amt_mm12) amt_mm12
FROM current_month
GROUP BY cif_funding
		, ecif
		, cif_cc
		, channel
)
SELECT 
		   scm.cif_funding
		   , scm.ecif
		   , scm.cif_cc
		   , scm.channel
		   , CASE WHEN dsctcm.flag_transaction IS NULL THEN 'New to feature'
              WHEN dsctcm.flag_transaction IS NOT NULL AND (IFNULL(dsctcm.cum_sum_not_feature,0) + scm.not_feature) = IFNULL(dsctcm.cum_sum_not_feature,0) THEN 'Churned'
              WHEN dsctcm.flag_transaction IS NOT NULL AND (IFNULL(dsctcm.cum_sum_not_feature,0) + scm.not_feature) > IFNULL(dsctcm.cum_sum_not_feature,0) AND dsctcm.flag_transaction <> 'Churned' THEN 'Stay'
              WHEN dsctcm.flag_transaction IS NOT NULL AND (IFNULL(dsctcm.cum_sum_not_feature,0) + scm.not_feature) > IFNULL(dsctcm.cum_sum_not_feature,0) AND dsctcm.flag_transaction = 'Churned' THEN 'Reactivate'
            END flag_transaction  
		   , scm.date_new_to_feature
		   , scm.last_trx_date
		   , scm.day_diff_last_trx
		   , scm.not_feature
		   , scm.amt_feature
		   , scm.cum_sum_not_feature
		   , scm.cum_amt_feature
		   , scm.not_mm1
		   , scm.not_mm2
		   , scm.not_mm3
		   , scm.not_mm4
		   , scm.not_mm5
		   , scm.not_mm6
		   , scm.not_mm7
		   , scm.not_mm8
		   , scm.not_mm9
		   , scm.not_mm10
		   , scm.not_mm11
		   , scm.not_mm12
		   , scm.amt_mm1
		   , scm.amt_mm2
		   , scm.amt_mm3
		   , scm.amt_mm4
		   , scm.amt_mm5
		   , scm.amt_mm6
		   , scm.amt_mm7
		   , scm.amt_mm8
		   , scm.amt_mm9
		   , scm.amt_mm10
		   , scm.amt_mm11
		   , scm.amt_mm12
		   , CAST(\'''' || cursor.EXECUTION_DATE || '''\' AS DATE) EXECUTION_DATE
		   , \'''' || V_JOB_ID || '''\' JOB_ID
		   , PARSE_DATETIME("%Y%m%d%H%M%S", \'''' || V_JOB_ID || '''\') JOB_ID_DATE_FORMAT
		   , CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE) BUSINESS_DATE
FROM summary_current_month scm
LEFT JOIN ( SELECT cif_funding
				  ,ecif
				  ,cif_cc
				  ,channel
				  ,cum_sum_not_feature
				  ,flag_transaction
			FROM
			dm_dap.dm_sum_channel_trx_cust_movement
			WHERE 1=1
			AND business_date='2021-12-31'
		  ) dsctcm
	 ON scm.cif_funding = dsctcm.cif_funding
	 AND IFNULL(scm.ecif,0) = IFNULL(dsctcm.ecif,0)
	 AND IFNULL(scm.cif_cc,0) = IFNULL(dsctcm.cif_cc,0) --UNTUK MENGATASI CASE CHANNEL SELAIN CC, JIKA CIF_CC NULL MAKA TIDAK AKAN DAPAT
	 AND scm.channel = dsctcm.channel
WHERE 1=1
--AND scm.cif_funding=13877164
--and scm.channel='SI'

















WITH current_month AS (
	SELECT cif_funding
		   , ecif
		   , cif_cc
		   , trx_type_level_2
		   , channel
		   , flag_transaction
		   , date_new_to_feature
		   , last_trx_date
		   , day_diff_last_trx
		   , not_feature
		   , amt_feature
		   , cum_sum_not_feature
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
	FROM dm_stg_dap.sum_trx_atmcdmcrm_cust_movement
	WHERE 1=1
	AND business_date = '2022-01-31'
	AND trx_type_level_2 <> 'Financial'
	UNION ALL
	SELECT cif_funding
		   , ecif
		   , cif_cc
		   , trx_type_level_2
		   , channel
		   , flag_transaction
		   , date_new_to_feature
		   , last_trx_date
		   , day_diff_last_trx
		   , not_feature
		   , amt_feature
		   , cum_sum_not_feature
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
	FROM dm_stg_dap.sum_trx_branch_cust_movement
	WHERE 1=1
	AND business_date = '2022-01-31'
	AND trx_type_level_2 <> 'Financial'
	UNION ALL
	SELECT cif_funding
		   , ecif
		   , cif_cc
		   , trx_type_level_2
		   , channel
		   , flag_transaction
		   , date_new_to_feature
		   , last_trx_date
		   , day_diff_last_trx
		   , not_feature
		   , amt_feature
		   , cum_sum_not_feature
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
	FROM dm_stg_dap.sum_trx_cc_cust_movement
	WHERE 1=1
	AND business_date = '2022-01-31'
	AND trx_type_level_2 <> 'Financial'
	UNION ALL
	SELECT cif_funding
		   , ecif
		   , cif_cc
		   , trx_type_level_2
		   , channel
		   , flag_transaction
		   , date_new_to_feature
		   , last_trx_date
		   , day_diff_last_trx
		   , not_feature
		   , amt_feature
		   , cum_sum_not_feature
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
	FROM dm_stg_dap.sum_trx_dbankpro_cust_movement
	WHERE 1=1
	AND business_date = '2022-01-31'
	AND trx_type_level_2 <> 'Financial'
	UNION ALL
	SELECT cif_funding
		   , ecif
		   , cif_cc
		   , trx_type_level_2
		   , channel
		   , flag_transaction
		   , date_new_to_feature
		   , last_trx_date
		   , day_diff_last_trx
		   , not_feature
		   , amt_feature
		   , cum_sum_not_feature
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
	FROM dm_stg_dap.sum_trx_dc_cust_movement
	WHERE 1=1
	AND business_date = '2022-01-31'
	AND trx_type_level_2 <> 'Financial'
	UNION ALL
	SELECT cif_funding
		   , ecif
		   , cif_cc
		   , trx_type_level_2
		   , channel
		   , flag_transaction
		   , date_new_to_feature
		   , last_trx_date
		   , day_diff_last_trx
		   , not_feature
		   , amt_feature
		   , cum_sum_not_feature
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
	FROM dm_stg_dap.sum_trx_dcc_cust_movement
	WHERE 1=1
	AND business_date = '2022-01-31'
	AND trx_type_level_2 <> 'Financial'
	UNION ALL
	SELECT cif_funding
		   , ecif
		   , cif_cc
		   , trx_type_level_2
		   , channel
		   , flag_transaction
		   , date_new_to_feature
		   , last_trx_date
		   , day_diff_last_trx
		   , not_feature
		   , amt_feature
		   , cum_sum_not_feature
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
	FROM dm_stg_dap.sum_trx_directdebit_cust_movement
	WHERE 1=1
	AND business_date = '2022-01-31'
	AND trx_type_level_2 <> 'Financial'
	UNION ALL
	SELECT cif_funding
		   , ecif
		   , cif_cc
		   , trx_type_level_2
		   , channel
		   , flag_transaction
		   , date_new_to_feature
		   , last_trx_date
		   , day_diff_last_trx
		   , not_feature
		   , amt_feature
		   , cum_sum_not_feature
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
	FROM dm_stg_dap.sum_trx_si_cust_movement
	WHERE 1=1
	AND business_date = '2022-01-31'
	AND trx_type_level_2 <> 'Financial'
)
--SELECT *
--	   , CAST('2022-12-31' AS DATE FORMAT 'RRRR-MM-DD')
--       , '20220131000000'
--       , CAST('20220131000000' AS DATETIME FORMAT 'RRRRMMDDHH24MISS')
--       , CAST('2022-01-31' AS DATE FORMAT 'RRRR-MM-DD')
--FROM current_month