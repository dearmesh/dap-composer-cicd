SELECT SAFE_CAST(cif AS INT64) cif
	  , NULL ecif
	  , NULL acctcust
	  , report_name product_holding
	  , SUM(CAST(principal_in_idr as NUMERIC)) osbal
	  , MIN(transaction_date) month_first_ph
    , MAX(CAST(transaction_date AS DATE FORMAT 'RRRR-MM-DD')) date_movement_ph
	  -- , expiry_date
	  -- , maturity_date_loan_acceptance
	  -- , date_pr
	  , SUM(CASE WHEN IFNULL(maturity_date_loan_acceptance,expiry_date) > date_pr THEN 1
	   	ELSE 0 END) STATUS_FLAG
FROM dm_dap.vw_loan_tfs
WHERE 1=1
--AND date_pr='2022-01-31'
AND SAFE_CAST(cif AS INT64) IN (3585539,14318377)
GROUP BY SAFE_CAST(cif AS INT64),report_name

SELECT *
FROM dm_dap.vw_loan_tfs
--WHERE SAFE_CAST(cif AS INT64)=304431
--GROUP BY cif,transaction_reff_number
--HAVING COUNT(*) > 1

SELECT SAFE_CAST(cif AS INT64) cif
			  , NULL ecif
			  , NULL acctcust
			  , report_name product_holding
			  , SUM(CAST(principal_in_idr as NUMERIC)) osbal
			  , MAX(SAFE_CAST(maturity_date_loan_acceptance AS DATE FORMAT 'RRRR-MM-DD')) maturity_date --MENGGUNAKAN SAFE CAST UNTUK VALUE EMPTY STRING/ TANGGAL YANG TIDAK TERPARSING MENJADI NULL
			  , MIN(transaction_date) month_first_ph
		      , MAX(CAST(transaction_date AS DATE FORMAT 'RRRR-MM-DD')) date_movement_ph
			  , SUM(CASE WHEN IFNULL(maturity_date_loan_acceptance,expiry_date) > date_pr THEN 1
			   	ELSE 0 END) STATUS_FLAG
		FROM dm_dap.vw_loan_tfs
		WHERE 1=1
		--AND date_pr='2022-01-31'
		AND SAFE_CAST(cif AS INT64) IN (3585539,14318377)
		GROUP BY SAFE_CAST(cif AS INT64),report_name

/** Butuh konfirmasi penggunaan groupnya , untuk sementara masih menggunakan product_group
 *  Untuk data cif terdapat value FAKE-E016424-P, untuk sementara menggunakan safe_cast
 *  Data Dalam bentuk Transaksi untuk data yang nonactive sudah tidak ada lagi di partitions setelahnya maka perlu mengambil data dari bulan sebelumnya
 */
WITH current_month AS (
    SELECT current_month.*, dcpgn.ecif
    FROM (
		SELECT SAFE_CAST(cif AS INT64) cif
--			  , NULL ecif
			  , NULL acctcust
			  , report_name product_holding
			  , SUM(CAST(principal_in_idr as NUMERIC)) osbal
			  , MAX(SAFE_CAST(maturity_date_loan_acceptance AS DATE FORMAT 'RRRR-MM-DD')) maturity_date --MENGGUNAKAN SAFE CAST UNTUK VALUE EMPTY STRING/ TANGGAL YANG TIDAK TERPARSING MENJADI NULL
			  , MIN(transaction_date) month_first_ph
		      , MAX(CAST(transaction_date AS DATE FORMAT 'RRRR-MM-DD')) date_movement_ph
			  , SUM(CASE WHEN IFNULL(maturity_date_loan_acceptance,expiry_date) < date_pr THEN 0 --JIKA INACTIVE MATURITY < DATE_PR 
			   	ELSE 1 END) STATUS_FLAG
		FROM dm_dap.vw_loan_tfs
		WHERE 1=1
		--AND date_pr='2022-01-31'
		AND SAFE_CAST(cif AS INT64) IN (3585539,14318377,1983536)
		GROUP BY SAFE_CAST(cif AS INT64),report_name
	)current_month
	LEFT JOIN (SELECT cif,ecif
			   FROM dm_dap.dm_customer_profile_general_new
			   WHERE 1=1
			   AND date_pr='20230430') dcpgn ON current_month.cif = dcpgn.cif
  )
  , current_month_check_active AS (
        SELECT CAST(cif AS INT64) cif
          ,CAST(ecif AS INT64) ecif
          ,product_holding
          ,CAST(month_first_ph as DATE) month_first_ph
          ,CASE WHEN STATUS_FLAG = 0 THEN CAST('2022-01-31' AS DATE) --- ISIKAN DENGAN BUSINESS DATE JIKA SEMUA ACCOUNT INACTIVE
            ELSE CAST(date_movement_ph AS DATE) END date_movement_ph
          ,maturity_date
          ,CAST(osbal AS NUMERIC) os_balance
          -- ,CAST(avg_balance AS NUMERIC) avg_balance
        FROM current_month
          WHERE 1=1
        UNION ALL
        SELECT 
              cif_funding cif
              ,ecif
              ,product_holding
              ,month_first_ph
              ,date_movement_ph
              ,maturity_date
              ,0 os_balance
      FROM dm_stg_dap.sum_portfolio_tf_cust_movement stdcm
      WHERE 1=1
      AND stdcm.BUSINESS_DATE='2021-12-31' --ambil data sum_trx_dcc di bulan sebelumnya 
  --    AND stdcm.trx_type_level_2 <> 'Financial'
      AND cif_funding||product_holding NOT IN (
        SELECT DISTINCT cif||product_holding FROM current_month ccm
      )  
  )
 , last_month AS (
        SELECT cmca.cif AS cif_funding
              ,cmca.ecif AS ecif
              ,NULL AS acctcust
              ,cmca.product_holding
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
              ,os_balance AS osbal_mm0
              ,CAST(NULL AS NUMERIC) AS avbal_mm0
          FROM current_month_check_active cmca
      LEFT JOIN (SELECT cif_funding
                      ,ecif
                      ,product_holding
                      ,month_first_ph
                      ,date_movement_ph
                      ,flag_ph
                      ,movement_interval_ph
                      ,maturity_date
                  FROM dm_stg_dap.sum_portfolio_tf_cust_movement 
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
              FROM dm_stg_dap.sum_portfolio_tf_cust_movement 
             WHERE 1=1
              AND business_date = '2021-12-31' --ambil data di bulan sebelumnya MONTH - 1
 --						  AND business_date = DATE_SUB(FORMAT_DATE('%Y%m%d', CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE)), INTERVAL 1 MONTH) --ambil data di bulan sebelumnya MONTH - 1
            ) spccm ON lm.cif_funding = spccm.cif_funding
                   AND lm.product_holding = spccm.product_holding


SELECT * FROM dm_dap.vw_loan_tfs

--vw_loan_tfs ?

SELECT * FROM dm_dap.dm_channel_trx_directdebit_new


SELECT * 
	FROM dm_dap.dm_loan_tfs 
	WHERE 1=1
--	AND date_pr='20221130'
--	AND date_pr IN ('20221130','20221231','20230306','20230403')
----	AND date_pr IN ('20230306','20230403')
--	AND cif='0003291761'
----	AND cif='0000304431'
--	AND product_group='OAF BUYER'
--	AND transaction_reff_number='L147897001'
--	AND transaction_reff_number='A030984001'
--	ORDER BY transaction_reff_number ,cif
--	GROUP BY cif,transaction_reff_number,date_pr
--	ORDER BY cif,transaction_reff_number,date_pr
	
SELECT * FROM dm_stg_dap.sum_portfolio_tf_cust_movement

SELECT * FROM dm_stg_dap.INFORMATION_SCHEMA.TABLES
WHERE table_name like '%mor%'

SELECT * FROM dm_dap.unit_link_bi

SELECT * FROM omt.omt_process_log
where src_file_or_tbl_name like '%unit_link_bi%'
--and execution_date='2022-01-01'
ORDER by execution_date 

