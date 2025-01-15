WITH cte_current_month AS ( SELECT CAST(dctdn.cif AS INT64) cif
                                   , CAST(dcpgn.ecif AS INT64) ecif
                                   , channel_type channel
                                   , trx_type_level_2
                                   , not_feature
                                   , amt_feature
                                   , date_new_to_feature
                                   , Last_trx_date  
                            FROM (SELECT dctdn.cif 
                                         , dctdn.mitra_name trx_type_level_2--, gccf.level2 trx_type_level_2
                                         , dctdn.channel_type
                                         , SUM(no_of_trx) not_feature
                                         , SUM(amount) amt_feature
                                         , MIN(trx_date) date_new_to_feature
                                         , MAX(trx_date) Last_trx_date 
                                  FROM dm_dap.dm_channel_trx_directdebit_new dctdn
                                 -- LEFT JOIN dm_dap.grouping_channel_cleansing_fbi  gccf ON UPPER(TRIM(dctdn.trx_type)) = UPPER(TRIM(gccf.trx_type))
                                  WHERE 1=1 
                                  --AND dctdn.date_pr = '20221102'
                                --  AND gccf.fin_type = 'Fin'
                                  AND dctdn.date_pr BETWEEN FORMAT_DATE('%Y%m01', CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE)) AND FORMAT_DATE('%Y%m%d', CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE)) --ambil data 1 bulan
                                  --AND dctdn.cif=15544
                                  AND dctdn.cif IS NOT NULL
                                  GROUP BY dctdn.cif
                                           , dctdn.mitra_name
                                          -- , gccf.level2
                                           , dctdn.channel_type
                                    ) dctdn
                            LEFT JOIN (SELECT * 
                                       FROM dm_dap.dm_customer_profile_general_new
                                       WHERE 1=1
                                       --AND date_pr = '20230430'
                                       AND date_pr = FORMAT_DATE('%Y%m%d', CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE))
                                       ) dcpgn ON CAST(dctdn.cif AS INT64) = dcpgn.cif 
    )
,   cte_grand_total_current_month AS (--------------- FINANCIAL SUM TOTAL PER CIF ---------------
                                      SELECT cif
                                             , ecif
                                             , channel
                                             , 'Financial' AS trx_type_level_2
                                             , '' flag_transaction
                                             , SUM(not_feature) not_feature
                                             , SUM(amt_feature) amt_feature
                                             , MIN(DATE(date_new_to_feature)) date_new_to_feature
                                             , MAX(DATE(last_trx_date)) last_trx_date
                                      FROM cte_current_month
                                      GROUP BY cif,ecif,channel
                                      UNION ALL 
                                      SELECT cif
                                             , ecif
                                             , Channel
                                             , trx_type_level_2
                                             , '' flag_transaction
                                             , not_feature
                                             , amt_feature
                                             , DATE(date_new_to_feature) date_new_to_feature
                                             , DATE(last_trx_date) last_trx_date
                                      FROM cte_current_month
    )
  --select * from cte_grand_total_current_month
,   union_current_month_with_last_month AS (SELECT cif
                                                   , ecif
                                                   , Channel
                                                   , trx_type_level_2
                                                   , '' flag_transaction
                                                   , not_feature
                                                   , amt_feature
                                                   , date_new_to_feature
                                                   , Last_trx_date 
                                            FROM cte_grand_total_current_month
                                            UNION ALL
                                            -- GET LAST MONTH PER CIF
                                            SELECT cif_funding cif
                                                   , ecif
                                                   , Channel
                                                   , trx_type_level_2
                                                   , flag_transaction
                                                   , 0 not_feature
                                                   , 0 amt_feature
                                                   , date_new_to_feature
                                                   , Last_trx_date 
                                            FROM dm_stg_dap.sum_trx_directdebit_cust_movement dctdn
                                            WHERE 1=1
                                            --AND dctdn.business_date = '2021-12-31' --ambil data sum_trx_dbankpro di bulan sebelumnya 
                                            AND dctdn.business_date = DATE_SUB(CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE), INTERVAL 1 MONTH) --ambil data sum_trx_dbankpro di bulan sebelumnya 
                                            AND cif_funding||trx_type_level_2 NOT IN ( SELECT DISTINCT cif||trx_type_level_2 
                                                                                       FROM cte_grand_total_current_month ccm )  
    )
,   cte_last_month AS (SELECT ucmwlm.cif
                              , ucmwlm.ecif
                              , ucmwlm.channel
                              , ucmwlm.trx_type_level_2
                              --  , stdcm.cum_sum_not_feature cum_not_feature_last_month
                              , CASE 
                                  WHEN stdcm.flag_transaction IS NULL THEN 'New to feature'
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
                        --------------- GET ALL NOT MM1 - MM12 --------------- 
                              , stdcm.not_feature not_mm1
                              , stdcm.not_mm1 not_mm2
                              , stdcm.not_mm2 not_mm3
                              , stdcm.not_mm3 not_mm4 
                              , stdcm.not_mm4 not_mm5
                              , stdcm.not_mm5 not_mm6
                              , stdcm.not_mm6 not_mm7
                              , stdcm.not_mm7 not_mm8
                              , stdcm.not_mm8 not_mm9
                              , stdcm.not_mm9 not_mm10
                              , stdcm.not_mm10 not_mm11
                              , stdcm.not_mm11 not_mm12
                              , stdcm.amt_feature amt_mm1
                              , stdcm.amt_mm1 amt_mm2
                              , stdcm.amt_mm2 amt_mm3
                              , stdcm.amt_mm3 amt_mm4 
                              , stdcm.amt_mm4 amt_mm5
                              , stdcm.amt_mm5 amt_mm6
                              , stdcm.amt_mm6 amt_mm7
                              , stdcm.amt_mm7 amt_mm8
                              , stdcm.amt_mm8 amt_mm9
                              , stdcm.amt_mm9 amt_mm10
                              , stdcm.amt_mm10 amt_mm11
                              , stdcm.amt_mm11 amt_mm12
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
                                    FROM dm_stg_dap.sum_trx_directdebit_cust_movement
                                    WHERE 1=1
                                    --AND business_date = '2021-12-31' --ambil data sum_trx_dbankpro di bulan sebelumnya
                                    AND business_date = DATE_SUB(CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE), INTERVAL 1 MONTH) --ambil data sum_trx_dbankpro di bulan sebelumnya 
                                    ) stdcm ON ucmwlm.cif = stdcm.cif_funding
                                            AND ucmwlm.trx_type_level_2 = stdcm.trx_type_level_2
  )
  SELECT CAST(cif AS INT64) cif_funding
  	     , ecif
  	     , NULL cif_cc
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
         , CAST(\'''' || cursor.EXECUTION_DATE || '''\' AS DATE) EXECUTION_DATE
         , \'''' || V_JOB_ID || '''\' JOB_ID
         , PARSE_DATETIME("%Y%m%d%H%M%S", \'''' || V_JOB_ID || '''\') JOB_ID_DATE_FORMAT
         , CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE) BUSINESS_DATE
    FROM cte_last_month