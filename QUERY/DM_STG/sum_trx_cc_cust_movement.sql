WITH cte_current_month AS (
    SELECT 
		CAST(dcpgn.cif AS INT64) cif
		,tctnt.ecif
		,tctnt.cif_cc
		,tctnt.trx_type_level_2
		,tctnt.not_feature
		,tctnt.amt_feature
		,tctnt.date_new_to_feature
		,tctnt.Last_trx_date
		, 'CC' Channel
	FROM (
		SELECT 
			CAST(tctnt.ecifref AS INT64) ecif
			,CAST(tctnt.acctcust AS INT64) cif_cc
			,tctnt.trxdesc trx_type_level_2
			,COUNT(acctcust) not_feature
			,SUM(tctnt.smttxnamt) amt_feature
			,MIN(CAST(CAST(tctnt.smtttxndte AS STRING) AS DATE FORMAT 'RRRRMMDD')) date_new_to_feature
			,MAX(CAST(CAST(tctnt.smtttxndte AS STRING) AS DATE FORMAT 'RRRRMMDD')) Last_trx_date
			, 'CC' Channel
		FROM dm_dap.t_cc_transactions_new_testing tctnt
		WHERE 1=1
--		AND tctnt.acctcust='2'
		AND tctnt.date_pr='20220131'
		GROUP BY tctnt.ecifref
				,tctnt.acctcust
				,tctnt.trxdesc
	) tctnt
	LEFT JOIN (
				SELECT CAST(cif AS INT64) cif
					   , CAST(ecif AS INT64) ecif 
				FROM dm_dap.dm_customer_profile_general_new
				WHERE 1=1
				AND date_pr='20230430'
	) dcpgn ON tctnt.ecif = dcpgn.ecif
  )
  , cte_grand_total_current_month AS (
    --FINANCIAL SUM TOTAL PER CIF
    SELECT cif
      ,ecif
      ,cif_cc
      ,'CC' channel
      ,'Financial' AS trx_type_level_2
      ,'' flag_transaction
      ,SUM(not_feature) not_feature
      ,SUM(amt_feature) amt_feature
      ,MIN(date_new_to_feature ) date_new_to_feature
      ,MAX(last_trx_date) last_trx_date
    FROM cte_current_month
    GROUP BY cif,ecif,cif_cc
      UNION ALL 
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
  )
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
      FROM cte_grand_total_current_month
      UNION ALL
      -- GET LAST MONTH PER CIF
      SELECT 
              cif_funding cif
              ,ecif
              ,cif_cc acctcust
              ,Channel
              ,trx_type_level_2
              ,flag_transaction
              ,0 not_feature
              ,0 amt_feature
              ,date_new_to_feature
              ,Last_trx_date 
      FROM dm_stg_dap.sum_trx_cc_cust_movement stccm
      WHERE 1=1
      AND stccm.BUSINESS_DATE='2021-12-31' --ambil data sum_trx_dcc di bulan sebelumnya 
  --    AND stdcm.trx_type_level_2 <> 'Financial'
      AND cif_cc||trx_type_level_2 NOT IN (
        SELECT DISTINCT cif_cc||trx_type_level_2 FROM cte_grand_total_current_month ccm
      )  
  )
  , cte_last_month AS (
    SELECT 
          ucmwlm.cif
         ,ucmwlm.ecif
         ,ucmwlm.cif_cc
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
      LEFT JOIN (SELECT 
                          cif_funding
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
                  dm_stg_dap.sum_trx_cc_cust_movement
                  WHERE 1=1
                  AND BUSINESS_DATE = '2021-12-31' --ambil data sum_trx_dcc di bulan sebelumnya
                ) stdcm ON ucmwlm.cif_cc = stdcm.cif_cc
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