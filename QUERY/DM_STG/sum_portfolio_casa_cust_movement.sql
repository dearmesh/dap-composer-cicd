WITH cte_current_month AS (
 SELECT cif  AS CIF_FUNDING
       ,ecif AS ECIF
       ,prod_group_level_2 AS PRODUCT_HOLDING
       ,DATE(acct_open_dt) AS MONTH_FIRST_PH
       ,CASE WHEN acct_status_group = 'CLOSED' THEN PARSE_DATE('%Y%m%d', date_pr)
        ELSE DATE(acct_open_dt) END AS DATE_MOVEMENT_PH 
       ,'' AS FLAG_PH
       ,SUM(balance_idr) AS Osbal_mm0
       ,SUM(fin_average_balance_idr) AS Avbal_mm0
   FROM dm_dap.dm_casa_funding
  WHERE 1=1
   AND date_pr BETWEEN '20230101' AND '20230331'
   AND cif = 15601
GROUP BY cif
       ,ecif 
       ,prod_group_level_2 
       ,DATE(acct_open_dt) 
       ,CASE WHEN acct_status_group = 'CLOSED' THEN PARSE_DATE('%Y%m%d', date_pr)
        ELSE DATE(acct_open_dt) END 
)
,union_current_month_with_last_month AS (
SELECT CAST(CIF_FUNDING AS INT64) CIF_FUNDING
      ,CAST(ECIF AS INT64) ECIF
      ,PRODUCT_HOLDING
      ,MONTH_FIRST_PH
      ,DATE_MOVEMENT_PH
      ,FLAG_PH
      ,CAST(Osbal_mm0 AS NUMERIC) Osbal_mm0
      ,CAST(Avbal_mm0 AS NUMERIC) Avbal_mm0
  FROM cte_current_month
UNION ALL
SELECT CIF_FUNDING
      ,ECIF
      ,PRODUCT_HOLDING
      ,MONTH_FIRST_PH
      ,DATE_MOVEMENT_PH
      ,FLAG_PH
      ,Osbal_mm0
      ,Avbal_mm0
  FROM dm_stg_dap.sum_portopolio_casa_cust_movement spccm
 WHERE 1=1
   AND spccm.business_date = '2023-03-31'
   AND NOT EXISTS (
      SELECT *
        FROM cte_current_month ccm
       WHERE 1=1
         AND spccm.cif_funding = CAST(ccm.cif_funding AS INT64)
         AND spccm.ecif = CAST(ccm.ecif AS INT64)
         AND spccm.product_holding = ccm.product_holding
    ) 
)
SELECT * FROM union_current_month_with_last_month



----------------------- VER 2.0 -----------------------
WITH current_month AS (
	SELECT cif
	      ,ecif
	      ,prod_group_level_2 product_holding
	      ,MIN(acct_open_dt) month_first_ph
	      ,MAX(acct_open_dt) date_movement_ph
	      ,SUM(CASE WHEN acct_status_group = 'ACTIVE' THEN 1
			   ELSE 0 END) num_status --JIKA > 0 THEN ACTIVE
		  ,SUM(balance_idr) os_balance
		  ,SUM(fin_average_balance_idr) avg_balance
	  FROM dm_dap.dm_casa_funding
	 WHERE 1=1
	   AND date_pr = '20230331'
	   AND cif = 13440
	--and cod_acct_no='000001129568'
	   AND prod_group_level_2 = 'Tabungan Danamon'
--	and acct_status_group='CLOSED'
  GROUP BY cif, ecif, prod_group_level_2
--	order by prod_group_level,acct_open_dt desc,date_pr
	--order by date_pr
)
, current_month_check_active AS (
	SELECT CAST(cif AS INT64) cif
		  ,CAST(ecif AS INT64) ecif
		  ,product_holding
		  ,CAST(month_first_ph as DATE) month_first_ph
		  ,CASE WHEN num_status = 0 THEN CAST('2023-03-31' AS DATE) --- nanti di isi dengan business date
		   ELSE CAST(date_movement_ph AS DATE) END date_movement_ph
		  ,CAST(os_balance AS NUMERIC) os_balance
		  ,CAST(avg_balance AS NUMERIC) avg_balance
	  FROM current_month
	 WHERE 1=1
)
, last_month AS (
  SELECT cmca.cif AS cif_funding
        ,cmca.ecif AS ecif
        ,cmca.product_holding
        ,cmca.month_first_ph
--        ,spccm.date_movement_ph
      	,cmca.date_movement_ph
        ,CASE WHEN spccm.date_movement_ph IS NULL THEN 'New PH'
	          WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay','Reactivate') THEN 'Stay'
	          WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay') THEN 'Churned'
	          WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN 'Churned'
	          WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN 'Reactivate'
          END flag_ph
        ,CASE WHEN spccm.date_movement_ph IS NULL THEN 1
	          WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay','Reactivate') THEN spccm.movement_interval_ph + 1
	          WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay') THEN 1
	          WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN spccm.movement_interval_ph + 1
--	          WHEN cmca.date_movement_ph = spccm.date_movement_ph THEN spccm.movement_interval_ph + 1
--	          WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay') THEN 1
	          WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN 1
          END movement_interval_ph
        ,os_balance AS asbal_mm0
        ,avg_balance AS avbal_mm0
    FROM current_month_check_active cmca
  LEFT JOIN (SELECT cif_funding
  				   ,ecif
  				   ,product_holding
  				   ,month_first_ph
  				   ,date_movement_ph
  				   ,flag_ph
  				   ,movement_interval_ph
               FROM dm_stg_dap.sum_portfolio_casa_cust_movement 
              WHERE 1=1
                AND BUSINESS_DATE = '2023-02-28' --ambil data di bulan sebelumnya MONTH - 1
             ) spccm ON cmca.cif = spccm.cif_funding
                    AND cmca.product_holding = spccm.product_holding
)
SELECT *
  FROM last_month


------------------------ VER SP BQ -----------------------------
WITH current_month AS (
                    	SELECT cif
                    	      ,ecif
                    	      ,prod_group_level_2 product_holding
                    	      ,MIN(acct_open_dt) month_first_ph
                    	      ,MAX(acct_open_dt) date_movement_ph
                    	      ,SUM(CASE WHEN acct_status_group = 'ACTIVE' THEN 1
                    			   ELSE 0 END) num_status --JIKA > 0 THEN ACTIVE
                    		  ,SUM(balance_idr) os_balance
                    		  ,SUM(fin_average_balance_idr) avg_balance
                    	  FROM dm_dap.dm_casa_funding
                    	 WHERE 1=1
--                    	   AND date_pr = '20230331'
--                    	   AND cif = 13440
--                    	   AND cod_acct_no='000001129568'
--                    	   AND prod_group_level_2 = 'Tabungan Danamon'
--                    	   AND acct_status_group='CLOSED'
                      GROUP BY cif, ecif, prod_group_level_2
                    --	order by prod_group_level,acct_open_dt desc,date_pr
                    	--order by date_pr
)
, current_month_check_active AS (
                                  SELECT CAST(cif AS INT64) cif
                                		,CAST(ecif AS INT64) ecif
                                		,product_holding
                                		,CAST(month_first_ph as DATE) month_first_ph
                                		,CASE WHEN num_status = 0 THEN CAST('2023-03-31' AS DATE) --- nanti di isi dengan business date
                                		 ELSE CAST(date_movement_ph AS DATE) END date_movement_ph
                                	    ,CAST(os_balance AS NUMERIC) os_balance
                                		,CAST(avg_balance AS NUMERIC) avg_balance
                                	FROM current_month
                                   WHERE 1=1
)
, last_month AS (
                 SELECT cmca.cif AS cif_funding
                       ,cmca.ecif AS ecif
                       ,NULL AS acctcust
                       ,cmca.product_holding
                       ,1 flag_individu
                       ,NULL AS maturity_date
                       ,cmca.month_first_ph
               --        ,spccm.date_movement_ph
                     	,cmca.date_movement_ph
                       ,CASE WHEN spccm.date_movement_ph IS NULL THEN 'New PH'
               	             WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay','Reactivate') THEN 'Stay'
               	             WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay') THEN 'Churned'
               	             WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN 'Churned'
               	             WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN 'Reactivate'
                         END flag_ph
                       ,CASE WHEN spccm.date_movement_ph IS NULL THEN 1
               	             WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay','Reactivate') THEN spccm.movement_interval_ph + 1
               	             WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay') THEN 1
               	             WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN spccm.movement_interval_ph + 1
               	             WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN 1
                         END movement_interval_ph
                       ,os_balance AS osbal_mm0
                       ,avg_balance AS avbal_mm0
                   FROM current_month_check_active cmca
              LEFT JOIN (SELECT cif_funding
                 			   ,ecif
                 			   ,product_holding
                 			   ,month_first_ph
                 			   ,date_movement_ph
                 			   ,flag_ph
                 			   ,movement_interval_ph
                           FROM dm_stg_dap.sum_portfolio_casa_cust_movement 
                          WHERE 1=1
--						    AND BUSINESS_DATE = '2023-02-28' --ambil data di bulan sebelumnya MONTH - 1
                         ) spccm ON cmca.cif = spccm.cif_funding
                                AND cmca.product_holding = spccm.product_holding
)
SELECT lm.* EXCEPT(osbal_mm0, avbal_mm0)
      ,lm.osbal_mm0 as osbal_mm0 
      ,spccm.osbal_mm0 as osbal_mm1
      ,spccm.osbal_mm1 as osbal_mm2
      ,spccm.osbal_mm2 as osbal_mm3
      ,spccm.osbal_mm3 as osbal_mm4
      ,spccm.osbal_mm4 as osbal_mm5
      ,spccm.osbal_mm5 as osbal_mm6
      ,spccm.osbal_mm6 as osbal_mm7
      ,spccm.osbal_mm7 as osbal_mm8
      ,spccm.osbal_mm8 as osbal_mm9
      ,spccm.osbal_mm9 as osbal_mm10
      ,spccm.osbal_mm10 as osbal_mm11
      ,spccm.osbal_mm11 as osbal_mm12
      ,lm.avbal_mm0 as avbal_mm0
      ,spccm.avbal_mm0 as avbal_mm1
      ,spccm.avbal_mm1 as avbal_mm2
      ,spccm.avbal_mm2 as avbal_mm3
      ,spccm.avbal_mm3 as avbal_mm4
      ,spccm.avbal_mm4 as avbal_mm5
      ,spccm.avbal_mm5 as avbal_mm6
      ,spccm.avbal_mm6 as avbal_mm7
      ,spccm.avbal_mm7 as avbal_mm8
      ,spccm.avbal_mm8 as avbal_mm9
      ,spccm.avbal_mm9 as avbal_mm10
      ,spccm.avbal_mm10 as avbal_mm11
      ,spccm.avbal_mm11 as avbal_mm12
  FROM last_month lm
LEFT JOIN (SELECT *
             FROM dm_stg_dap.sum_portfolio_casa_cust_movement 
            WHERE 1=1
--              AND BUSINESS_DATE = '2023-02-28' --ambil data di bulan sebelumnya MONTH - 1
           ) spccm ON lm.cif_funding = spccm.cif_funding
                  AND lm.product_holding = spccm.product_holding

------------------------ VER 2.0 SP BQ -----------------------------
WITH non_multiple AS (
                      SELECT CONCAT(cif, '|', prod_group_level_1) AS combined_key
                        FROM dm_dap.dm_casa_funding
                       WHERE 1=1
                         AND date_pr = '20230331'
--                         AND date_pr = FORMAT_DATE('%Y%m%d', CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE))
                    GROUP BY cif, prod_group_level_1
                      HAVING COUNT(prod_group_level_1) = 1
)
, current_month AS (
---------------------------- UNTUK DATA NON MULTIPLE ACCOUNT ---------------------------- 
SELECT dcf.cif
      ,dcf.ecif
      ,dcf.prod_group_level_1 AS product_holding
--      ,dcf.acct_status_group
      ,MIN(dcf.acct_open_dt) AS month_first_ph
      ,MAX(dcf.acct_open_dt) AS date_movement_ph
      ,SUM(CASE WHEN dcf.acct_status_group = 'ACTIVE' THEN 1 ELSE 0 END) AS num_status -- Jika > 0 maka ACTIVE
      ,SUM(CASE WHEN dcf.acct_status_group <> 'CLOSED' THEN dcf.balance_idr ELSE 0 END) AS os_balance
      ,SUM(CASE WHEN prev.acct_status_group <> 'CLOSED' THEN prev.fin_average_balance_idr ELSE 0 END) AS avg_balance -- Nilai rata-rata bulan sebelumnya
--  ,dcf.date_pr
--  ,prev.date_pr
  FROM dm_dap.dm_casa_funding dcf
LEFT JOIN (SELECT cif, prod_group_level_1, acct_status_group, fin_average_balance_idr
             FROM dm_dap.dm_casa_funding
            WHERE 1=1
              AND date_pr = '20230228'  --- bulan sebelumnya
--              AND date_pr = DATE_SUB(FORMAT_DATE('%Y%m%d', CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE)), INTERVAL 1 MONTH) --ambil data di bulan sebelumnya MONTH - 1
          ) prev ON 1=1
                AND dcf.cif = prev.cif
                AND dcf.prod_group_level_1 = prev.prod_group_level_1
 WHERE 1=1
   AND dcf.date_pr = '20230331'
--   AND dcf.date_pr = FORMAT_DATE('%Y%m%d', CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE))
   AND CONCAT(dcf.cif, '|', dcf.prod_group_level_1) IN (SELECT combined_key FROM non_multiple)
GROUP BY dcf.cif, dcf.ecif, dcf.prod_group_level_1
UNION ALL
---------------------------- UNTUK DATA MULTIPLE ACCOUNT ---------------------------- 
SELECT dcf.cif
      ,dcf.ecif
      ,dcf.prod_group_level_1 AS product_holding
      ,MIN(dcf.acct_open_dt) AS month_first_ph
      ,MAX(dcf.acct_open_dt) AS date_movement_ph
--      ,CASE WHEN num_status = 0 THEN CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE) --- nanti di isi dengan business date
      ,SUM(CASE WHEN dcf.acct_status_group = 'ACTIVE' THEN 1 ELSE 0 END) AS num_status -- Jika > 0 maka ACTIVE
      ,SUM(dcf.balance_idr) AS os_balance
      ,SUM(dcf.fin_average_balance_idr) AS avg_balance
  FROM dm_dap.dm_casa_funding dcf
 WHERE 1=1
   AND dcf.date_pr = '20230331'
--   AND dcf.date_pr = FORMAT_DATE('%Y%m%d', CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE))
   AND CONCAT(dcf.cif, '|', dcf.prod_group_level_1) NOT IN (SELECT combined_key FROM non_multiple)
GROUP BY dcf.cif, dcf.ecif, dcf.prod_group_level_1
)
, current_month_check_active AS (
                                  SELECT CAST(cif AS INT64) cif
                                		,CAST(ecif AS INT64) ecif
                                		,product_holding
                                		,CAST(month_first_ph as DATE) month_first_ph
                                		,CASE WHEN num_status = 0 THEN CAST('2023-03-31' AS DATE) --- nanti di isi dengan business date
                                		 ELSE CAST(date_movement_ph AS DATE) END date_movement_ph
                                	    ,CAST(os_balance AS NUMERIC) os_balance
                                		,CAST(avg_balance AS NUMERIC) avg_balance
                                	FROM current_month
                                   WHERE 1=1
)
, last_month AS (
                   SELECT cmca.cif AS cif_funding
                         ,cmca.ecif AS ecif
                         ,NULL AS acctcust
                         ,cmca.product_holding
                         ,1 flag_individu
                         ,CAST(NULL AS DATE) AS maturity_date
                         ,cmca.month_first_ph
                       	 ,cmca.date_movement_ph
                         ,CASE WHEN spccm.date_movement_ph IS NULL THEN 'New PH'
                 	             WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay','Reactivate') THEN 'Stay'
                 	             WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay') THEN 'Churned'
                 	             WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN 'Churned'
                 	             WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN 'Reactivate'
                           END flag_ph
                         ,CASE WHEN spccm.date_movement_ph IS NULL THEN 1
                 	             WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay','Reactivate') THEN spccm.movement_interval_ph + 1
                 	             WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay') THEN 1
                 	             WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN spccm.movement_interval_ph + 1
                 	             WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN 1
                           END movement_interval_ph
                         ,os_balance AS osbal_mm0
                         ,avg_balance AS avbal_mm0
                     FROM current_month_check_active cmca
                LEFT JOIN (SELECT cif_funding
                                 ,ecif
                                 ,product_holding
                                 ,month_first_ph
                                 ,date_movement_ph
                                 ,flag_ph
                                 ,movement_interval_ph
                             FROM dm_stg_dap.sum_portfolio_casa_cust_movement 
                            WHERE 1=1
  --						    AND business_date = '2023-02-28' --ambil data di bulan sebelumnya MONTH - 1
  --						    AND business_date = DATE_SUB(FORMAT_DATE('%Y%m%d', CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE)), INTERVAL 1 MONTH) --ambil data di bulan sebelumnya MONTH - 1
                           ) spccm ON cmca.cif = spccm.cif_funding
                                  AND cmca.product_holding = spccm.product_holding
  )
    SELECT lm.* EXCEPT(osbal_mm0, avbal_mm0)
        ,lm.osbal_mm0 as osbal_mm0 
        ,spccm.osbal_mm0 as osbal_mm1
        ,spccm.osbal_mm1 as osbal_mm2
        ,spccm.osbal_mm2 as osbal_mm3
        ,spccm.osbal_mm3 as osbal_mm4
        ,spccm.osbal_mm4 as osbal_mm5
        ,spccm.osbal_mm5 as osbal_mm6
        ,spccm.osbal_mm6 as osbal_mm7
        ,spccm.osbal_mm7 as osbal_mm8
        ,spccm.osbal_mm8 as osbal_mm9
        ,spccm.osbal_mm9 as osbal_mm10
        ,spccm.osbal_mm10 as osbal_mm11
        ,spccm.osbal_mm11 as osbal_mm12
        ,lm.avbal_mm0 as avbal_mm0
        ,spccm.avbal_mm0 as avbal_mm1
        ,spccm.avbal_mm1 as avbal_mm2
        ,spccm.avbal_mm2 as avbal_mm3
        ,spccm.avbal_mm3 as avbal_mm4
        ,spccm.avbal_mm4 as avbal_mm5
        ,spccm.avbal_mm5 as avbal_mm6
        ,spccm.avbal_mm6 as avbal_mm7
        ,spccm.avbal_mm7 as avbal_mm8
        ,spccm.avbal_mm8 as avbal_mm9
        ,spccm.avbal_mm9 as avbal_mm10
        ,spccm.avbal_mm10 as avbal_mm11
        ,spccm.avbal_mm11 as avbal_mm12
--        , CAST(\'''' || cursor.EXECUTION_DATE || '''\' AS DATE) EXECUTION_DATE
--        , \'''' || V_JOB_ID || '''\' JOB_ID
--        , PARSE_DATETIME("%Y%m%d%H%M%S", \'''' || V_JOB_ID || '''\') JOB_ID_DATE_FORMAT
--        , CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE) BUSINESS_DATE
    FROM last_month lm
  LEFT JOIN (SELECT *
               FROM dm_stg_dap.sum_portfolio_casa_cust_movement 
              WHERE 1=1
--                AND business_date = '2023-02-28' --ambil data di bulan sebelumnya MONTH - 1
--  			    AND business_date = DATE_SUB(FORMAT_DATE('%Y%m%d', CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE)), INTERVAL 1 MONTH) --ambil data di bulan sebelumnya MONTH - 1
             ) spccm ON lm.cif_funding = spccm.cif_funding
                    AND lm.product_holding = spccm.product_holding


------------------------ VER 3.0 SP BQ -----------------------------
WITH current_month AS (
SELECT dcf.cif
      ,dcf.ecif
      ,dcf.prod_group_level_1 AS product_holding
--      ,dcf.acct_status_group
      ,MIN(dcf.acct_open_dt) AS month_first_ph
      ,MAX(dcf.acct_open_dt) AS date_movement_ph
      ,SUM(CASE WHEN dcf.acct_status_group = 'ACTIVE' THEN 1 ELSE 0 END) AS num_status -- Jika > 0 maka ACTIVE
      ,SUM(CASE WHEN dcf.acct_status_group <> 'CLOSED' THEN dcf.balance_idr ELSE 0 END) AS os_balance
      ,SUM(CASE WHEN dcf.acct_status_group <> 'CLOSED' THEN dcf.fin_average_balance_idr ELSE dcf.fin_average_balance_idr END) AS avg_balance
  FROM dm_dap.dm_casa_funding dcf
 WHERE 1=1
--   AND dcf.date_pr = 
GROUP BY dcf.cif, dcf.ecif, dcf.prod_group_level_1
)
, current_month_check_active AS (
                                  SELECT CAST(cif AS INT64) cif
                                		,CAST(ecif AS INT64) ecif
                                		,product_holding
                                		,CAST(month_first_ph as DATE) month_first_ph
                                		,CASE WHEN num_status = 0 THEN CAST('2023-03-31' AS DATE) --- nanti di isi dengan business date
                                		 ELSE CAST(date_movement_ph AS DATE) END date_movement_ph
                                	    ,CAST(os_balance AS NUMERIC) os_balance
                                		,CAST(avg_balance AS NUMERIC) avg_balance
                                	FROM current_month
                                   WHERE 1=1
)
, last_month AS (
                   SELECT cmca.cif AS cif_funding
                         ,cmca.ecif AS ecif
                         ,NULL AS acctcust
                         ,cmca.product_holding
                         ,1 flag_individu
                         ,CAST(NULL AS DATE) AS maturity_date
                         ,cmca.month_first_ph
                       	 ,cmca.date_movement_ph
                         ,CASE WHEN spccm.date_movement_ph IS NULL THEN 'New PH'
                 	             WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay','Reactivate') THEN 'Stay'
                 	             WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay') THEN 'Churned'
                 	             WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN 'Churned'
                 	             WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN 'Reactivate'
                           END flag_ph
                         ,CASE WHEN spccm.date_movement_ph IS NULL THEN 1
                 	             WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay','Reactivate') THEN spccm.movement_interval_ph + 1
                 	             WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay') THEN 1
                 	             WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN spccm.movement_interval_ph + 1
                 	             WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN 1
                           END movement_interval_ph
                         ,os_balance AS osbal_mm0
                         ,avg_balance AS avbal_mm0
                     FROM current_month_check_active cmca
                LEFT JOIN (SELECT cif_funding
                                 ,ecif
                                 ,product_holding
                                 ,month_first_ph
                                 ,date_movement_ph
                                 ,flag_ph
                                 ,movement_interval_ph
                             FROM dm_stg_dap.sum_portfolio_casa_cust_movement 
                            WHERE 1=1
  --						    AND business_date = '2023-02-28' --ambil data di bulan sebelumnya MONTH - 1
  --						    AND business_date = DATE_SUB(FORMAT_DATE('%Y%m%d', CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE)), INTERVAL 1 MONTH) --ambil data di bulan sebelumnya MONTH - 1
                           ) spccm ON cmca.cif = spccm.cif_funding
                                  AND cmca.product_holding = spccm.product_holding
  )
    SELECT lm.* EXCEPT(osbal_mm0, avbal_mm0)
        ,lm.osbal_mm0 as osbal_mm0 
        ,spccm.osbal_mm0 as osbal_mm1
        ,spccm.osbal_mm1 as osbal_mm2
        ,spccm.osbal_mm2 as osbal_mm3
        ,spccm.osbal_mm3 as osbal_mm4
        ,spccm.osbal_mm4 as osbal_mm5
        ,spccm.osbal_mm5 as osbal_mm6
        ,spccm.osbal_mm6 as osbal_mm7
        ,spccm.osbal_mm7 as osbal_mm8
        ,spccm.osbal_mm8 as osbal_mm9
        ,spccm.osbal_mm9 as osbal_mm10
        ,spccm.osbal_mm10 as osbal_mm11
        ,spccm.osbal_mm11 as osbal_mm12
        ,lm.avbal_mm0 as avbal_mm0
        ,spccm.avbal_mm0 as avbal_mm1
        ,spccm.avbal_mm1 as avbal_mm2
        ,spccm.avbal_mm2 as avbal_mm3
        ,spccm.avbal_mm3 as avbal_mm4
        ,spccm.avbal_mm4 as avbal_mm5
        ,spccm.avbal_mm5 as avbal_mm6
        ,spccm.avbal_mm6 as avbal_mm7
        ,spccm.avbal_mm7 as avbal_mm8
        ,spccm.avbal_mm8 as avbal_mm9
        ,spccm.avbal_mm9 as avbal_mm10
        ,spccm.avbal_mm10 as avbal_mm11
        ,spccm.avbal_mm11 as avbal_mm12
--        , CAST(\'''' || cursor.EXECUTION_DATE || '''\' AS DATE) EXECUTION_DATE
--        , \'''' || V_JOB_ID || '''\' JOB_ID
--        , PARSE_DATETIME("%Y%m%d%H%M%S", \'''' || V_JOB_ID || '''\') JOB_ID_DATE_FORMAT
--        , CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE) BUSINESS_DATE
    FROM last_month lm
  LEFT JOIN (SELECT *
               FROM dm_stg_dap.sum_portfolio_casa_cust_movement 
              WHERE 1=1
--                AND business_date = '2023-02-28' --ambil data di bulan sebelumnya MONTH - 1
--  			    AND business_date = DATE_SUB(FORMAT_DATE('%Y%m%d', CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE)), INTERVAL 1 MONTH) --ambil data di bulan sebelumnya MONTH - 1
             ) spccm ON lm.cif_funding = spccm.cif_funding
                    AND lm.product_holding = spccm.product_holding


------------------------ VER 4.0 SP BQ -----------------------------

WITH current_month AS (
SELECT dcf.cif
      ,dcf.ecif
      ,dcf.prod_group_level_1 AS product_holding
--      ,dcf.acct_status_group
      ,MIN(dcf.acct_open_dt) AS month_first_ph
      ,MAX(dcf.acct_open_dt) AS date_movement_ph
      ,SUM(CASE WHEN dcf.acct_status_group = 'ACTIVE' THEN 1 ELSE 0 END) AS num_status -- Jika > 0 maka ACTIVE
      ,SUM(CASE WHEN dcf.acct_status_group <> 'CLOSED' THEN dcf.balance_idr ELSE 0 END) AS os_balance
      ,SUM(CASE WHEN dcf.acct_status_group <> 'CLOSED' THEN dcf.fin_average_balance_idr 
                WHEN prev.acct_status_group = 'CLOSED' AND dcf.acct_status_group = 'CLOSED' THEN 0
                ELSE dcf.fin_average_balance_idr END) AS avg_balance
  FROM dm_dap.dm_casa_funding dcf
LEFT JOIN (SELECT cif, prod_group_level_1, acct_status_group, fin_average_balance_idr
             FROM dm_dap.dm_casa_funding
            WHERE 1=1
--              AND date_pr = '20230228'  --- bulan sebelumnya
--              AND date_pr = DATE_SUB(FORMAT_DATE('%Y%m%d', CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE)), INTERVAL 1 MONTH) --ambil data di bulan sebelumnya MONTH - 1
          ) prev 
       ON 1=1
      AND dcf.cif = prev.cif
      AND dcf.prod_group_level_1 = prev.prod_group_level_1
 WHERE 1=1
--   AND dcf.date_pr = FORMAT_DATE('%Y%m%d', CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE))                
GROUP BY dcf.cif, dcf.ecif, dcf.prod_group_level_1
)
, current_month_check_active AS (
                                  SELECT CAST(cif AS INT64) cif
                                		,CAST(ecif AS INT64) ecif
                                		,product_holding
                                		,CAST(month_first_ph as DATE) month_first_ph
                                		,CASE WHEN num_status = 0 THEN CAST('2023-03-31' AS DATE) --- nanti di isi dengan business date
                                		 ELSE CAST(date_movement_ph AS DATE) END date_movement_ph
                                	    ,CAST(os_balance AS NUMERIC) os_balance
                                		,CAST(avg_balance AS NUMERIC) avg_balance
                                	FROM current_month
                                   WHERE 1=1
)
, last_month AS (
                   SELECT cmca.cif AS cif_funding
                         ,cmca.ecif AS ecif
                         ,NULL AS acctcust
                         ,cmca.product_holding
                         ,1 flag_individu
                         ,CAST(NULL AS DATE) AS maturity_date
                         ,cmca.month_first_ph
                       	 ,cmca.date_movement_ph
                         ,CASE WHEN spccm.date_movement_ph IS NULL THEN 'New PH'
                 	             WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay','Reactivate') THEN 'Stay'
                 	             WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay') THEN 'Churned'
                 	             WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN 'Churned'
                 	             WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN 'Reactivate'
                           END flag_ph
                         ,CASE WHEN spccm.date_movement_ph IS NULL THEN 1
                 	             WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay','Reactivate') THEN spccm.movement_interval_ph + 1
                 	             WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph IN ('New PH','Stay') THEN 1
                 	             WHEN cmca.date_movement_ph = spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN spccm.movement_interval_ph + 1
                 	             WHEN cmca.date_movement_ph > spccm.date_movement_ph AND spccm.flag_ph='Churned' THEN 1
                           END movement_interval_ph
                         ,os_balance AS osbal_mm0
                         ,avg_balance AS avbal_mm0
                     FROM current_month_check_active cmca
                LEFT JOIN (SELECT cif_funding
                                 ,ecif
                                 ,product_holding
                                 ,month_first_ph
                                 ,date_movement_ph
                                 ,flag_ph
                                 ,movement_interval_ph
                             FROM dm_stg_dap.sum_portfolio_casa_cust_movement 
                            WHERE 1=1
  --						    AND business_date = '2023-02-28' --ambil data di bulan sebelumnya MONTH - 1
  --						    AND business_date = DATE_SUB(FORMAT_DATE('%Y%m%d', CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE)), INTERVAL 1 MONTH) --ambil data di bulan sebelumnya MONTH - 1
                           ) spccm ON cmca.cif = spccm.cif_funding
                                  AND cmca.product_holding = spccm.product_holding
  )
    SELECT lm.* EXCEPT(osbal_mm0, avbal_mm0)
        ,lm.osbal_mm0 as osbal_mm0 
        ,spccm.osbal_mm0 as osbal_mm1
        ,spccm.osbal_mm1 as osbal_mm2
        ,spccm.osbal_mm2 as osbal_mm3
        ,spccm.osbal_mm3 as osbal_mm4
        ,spccm.osbal_mm4 as osbal_mm5
        ,spccm.osbal_mm5 as osbal_mm6
        ,spccm.osbal_mm6 as osbal_mm7
        ,spccm.osbal_mm7 as osbal_mm8
        ,spccm.osbal_mm8 as osbal_mm9
        ,spccm.osbal_mm9 as osbal_mm10
        ,spccm.osbal_mm10 as osbal_mm11
        ,spccm.osbal_mm11 as osbal_mm12
        ,lm.avbal_mm0 as avbal_mm0
        ,spccm.avbal_mm0 as avbal_mm1
        ,spccm.avbal_mm1 as avbal_mm2
        ,spccm.avbal_mm2 as avbal_mm3
        ,spccm.avbal_mm3 as avbal_mm4
        ,spccm.avbal_mm4 as avbal_mm5
        ,spccm.avbal_mm5 as avbal_mm6
        ,spccm.avbal_mm6 as avbal_mm7
        ,spccm.avbal_mm7 as avbal_mm8
        ,spccm.avbal_mm8 as avbal_mm9
        ,spccm.avbal_mm9 as avbal_mm10
        ,spccm.avbal_mm10 as avbal_mm11
        ,spccm.avbal_mm11 as avbal_mm12
--        , CAST(\'''' || cursor.EXECUTION_DATE || '''\' AS DATE) EXECUTION_DATE
--        , \'''' || V_JOB_ID || '''\' JOB_ID
--        , PARSE_DATETIME("%Y%m%d%H%M%S", \'''' || V_JOB_ID || '''\') JOB_ID_DATE_FORMAT
--        , CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE) BUSINESS_DATE
    FROM last_month lm
  LEFT JOIN (SELECT *
               FROM dm_stg_dap.sum_portfolio_casa_cust_movement 
              WHERE 1=1
--                AND business_date = '2023-02-28' --ambil data di bulan sebelumnya MONTH - 1
--  			    AND business_date = DATE_SUB(FORMAT_DATE('%Y%m%d', CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE)), INTERVAL 1 MONTH) --ambil data di bulan sebelumnya MONTH - 1
             ) spccm ON lm.cif_funding = spccm.cif_funding
                    AND lm.product_holding = spccm.product_holding