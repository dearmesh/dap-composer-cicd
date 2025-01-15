WITH current_month AS (
        SELECT current_month.*, dcpgn.ecif
        FROM (
        SELECT dcf.cif
              -- ,CAST(NULL AS INT64) ecif
              ,dcf.product_name AS product_holding
              ,MIN(dcf.open_date) AS month_first_ph
              ,MAX(dcf.open_date) AS date_movement_ph
              ,MAX(CAST(dcf.maturity_date AS DATE)) maturity_date
              ,SUM(CASE WHEN dcf.STATUS = 'INACTIVE' THEN 0 ELSE 1 END) AS num_status -- Jika > 0 maka ACTIVE
              ,SUM(CASE WHEN dcf.STATUS <> 'INACTIVE' THEN dcf.os ELSE 0 END) AS os_balance
      --        ,SUM(CASE WHEN dcf.STATUS <> 'INACTIVE' THEN dcf.fin_average_balance_idr ELSE dcf.fin_average_balance_idr END) AS avg_balance 
          FROM dm_dap.dm_disburse_mortgage dcf
--          FROM test.dm_disburse_mortgage dcf
        WHERE 1=1
            AND dcf.date_pr = '20220531'
            AND dcf.cif=14397843
          --  AND dcf.date_pr = FORMAT_DATE('%Y%m%d', CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE))
        GROUP BY dcf.cif, dcf.product_name
        )current_month
        LEFT JOIN (SELECT cif,ecif
			  FROM dm_dap.dm_customer_profile_general_new
			  WHERE 1=1
			  AND date_pr='20230430') dcpgn ON current_month.cif = dcpgn.cif
  )
  , current_month_check_active AS (
        SELECT CAST(cmca.cif AS INT64) cif
          ,cmca.ecif
          ,cmca.product_holding
          ,CAST(cmca.month_first_ph as DATE) month_first_ph
          ,CASE WHEN cmca.num_status = 0 AND spccm.flag_ph <> 'Churned'  THEN CAST('2022-01-31' AS DATE) --- nanti di isi dengan business date
          		WHEN cmca.num_status = 0 AND spccm.flag_ph = 'Churned'  THEN spccm.date_movement_ph
            ELSE CAST(cmca.date_movement_ph AS DATE) END date_movement_ph
          ,CAST(cmca.os_balance AS NUMERIC) os_balance
          ,cmca.maturity_date
          -- ,CAST(avg_balance AS NUMERIC) avg_balance
        FROM current_month cmca
        LEFT JOIN (SELECT cif_funding
                      ,ecif
                      ,product_holding
                      ,month_first_ph
                      ,date_movement_ph
                      ,flag_ph
                      ,movement_interval_ph
                      ,maturity_date
                  FROM dm_stg_dap.sum_portfolio_mortgage_cust_movement 
                  WHERE 1=1
      AND business_date = '2021-12-31' --ambil data di bulan sebelumnya MONTH - 1
--						    AND business_date = DATE_SUB(FORMAT_DATE('%Y%m%d', CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE)), INTERVAL 1 MONTH) --ambil data di bulan sebelumnya MONTH - 1
                ) spccm ON cmca.cif = spccm.cif_funding
                        AND cmca.product_holding = spccm.product_holding
          WHERE 1=1
  )
 , last_month AS (
        SELECT cmca.cif AS cif_funding
              ,cmca.ecif AS ecif
              ,NULL AS acctcust
              ,cmca.product_holding
              -- ,1 flag_individu
              ,IFNULL(cmca.maturity_date,spccm.maturity_date) maturity_date
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
              ,cmca.date_movement_ph
              ,spccm.date_movement_ph
              ,os_balance AS osbal_mm0
              ,0 AS avbal_mm0
          FROM current_month_check_active cmca
      LEFT JOIN (SELECT cif_funding
                      ,ecif
                      ,product_holding
                      ,month_first_ph
                      ,date_movement_ph
                      ,flag_ph
                      ,movement_interval_ph
                      ,maturity_date
                  FROM dm_stg_dap.sum_portfolio_mortgage_cust_movement 
                  WHERE 1=1
      AND business_date = '2021-12-31' --ambil data di bulan sebelumnya MONTH - 1
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
       , CAST('2022-12-31' AS DATE FORMAT 'RRRR-MM-DD')
       , '20220131000000'
       , CAST('20220131000000' AS DATETIME FORMAT 'RRRRMMDDHH24MISS')
       , CAST('2022-01-31' AS DATE FORMAT 'RRRR-MM-DD')
   FROM last_month lm
 LEFT JOIN (SELECT *
              FROM dm_stg_dap.sum_portfolio_mortgage_cust_movement 
             WHERE 1=1
              AND business_date = '2021-12-31' --ambil data di bulan sebelumnya MONTH - 1
 --						  AND business_date = DATE_SUB(FORMAT_DATE('%Y%m%d', CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE)), INTERVAL 1 MONTH) --ambil data di bulan sebelumnya MONTH - 1
            ) spccm ON lm.cif_funding = spccm.cif_funding
                   AND lm.product_holding = spccm.product_holding

SELECT * FROM dm_dap.dm_disburse_mortgage
WHERE 1=1
AND date_pr = '20220512'
--AND cif=4891512
--GROUP BY cif 
--having count(*) > 1

SELECT * FROM dm_dap.new_investment_fx
WHERE date_pr='20220331'