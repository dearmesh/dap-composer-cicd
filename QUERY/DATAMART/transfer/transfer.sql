CREATE OR REPLACE PROCEDURE `prj-7810ed85d543e33a.sp_dap.sp_sum_trx_counterparty_cust`(V_JOB_TYPE STRING, V_JOB_NAME STRING, V_PROJECT_ID STRING, V_DATASET_ID STRING, V_TARGET_TABLE STRING, V_EXECUTION_DATE DATE, V_JOB_ID STRING, V_OMT_JOB_DEPENDENCY_TABLE_NAME STRING, V_OMT_PROCESS_LOG_TABLE_NAME STRING, V_AIRFLOW_VAR_DAP_DATASET_TEMP_SP STRING)
BEGIN
                                                                 
-- CREATE by   : MII
-- UPDATE Date : 07 Desember 2024
-- COMMENTS    : SP untuk load sum_trx_counterparty_cust


--CREATE TABLE temp_sp_dap DEPENDENCY
EXECUTE IMMEDIATE '''
CREATE TEMPORARY TABLE temp_cursor_sp_sum_trx_counterparty_cust
AS  
SELECT PROCESS_BUSINESS_DATE, EXECUTION_DATE
FROM ''' || V_AIRFLOW_VAR_DAP_DATASET_TEMP_SP || '.' || V_TARGET_TABLE || '''_business_date_list
''';

--1. Kita akan melakukan For Loop process Query BUSINESS_DATE nya sebanyak BUSINESS_DATE yang sudah di list di table temp_cursor_sp_XXXX
FOR cursor IN (SELECT 
                PROCESS_BUSINESS_DATE
                , EXECUTION_DATE 
               FROM temp_cursor_sp_sum_trx_counterparty_cust
              )
DO

  --2. Update OMT Status hanya untuk yang ERROR menjadi RUNNING
  EXECUTE IMMEDIATE '''
  UPDATE `''' || V_PROJECT_ID || '`.' || V_OMT_PROCESS_LOG_TABLE_NAME || '''
  SET STATUS="RUNNING" 
    , STATUS_DESCRIPTION = NULL
  WHERE 1=1 
      AND TRG_TBL_NAME = \''''|| V_PROJECT_ID || '.' || V_DATASET_ID || '.' || V_TARGET_TABLE || '''\'
      AND JOB_TYPE = \'''' || V_JOB_TYPE || '''\'
      AND JOB_NAME = \'''' || V_JOB_NAME || '''\'
      AND EXECUTION_DATE = \'''' || cursor.EXECUTION_DATE || '''\'
      AND BUSINESS_DATE = \'''' || cursor.PROCESS_BUSINESS_DATE || '''\'
      AND STATUS = "ERROR"
  '''
  ;

  --3. Delete Partition yang akan diproses, untuk make sure data clean
  EXECUTE IMMEDIATE '''
  DELETE `''' || V_PROJECT_ID || '`.' || V_DATASET_ID || '.' || V_TARGET_TABLE || '''
  WHERE 1=1
    AND BUSINESS_DATE = \'''' || cursor.PROCESS_BUSINESS_DATE || '''\'
  '''
  ;

  --4. Mulai Proses query utama Datamart
  --================================================================

    --==========================trf_bifast======================================
   EXECUTE IMMEDIATE '''
	CREATE TEMPORARY TABLE trf_bifast AS 
	WITH bank_code AS (SELECT lpad(CAST(cod_bank AS STRING), 6,'0') bank_code
							, cod_sw_bank
							, nam_bank_alias
						FROM `prj-7810ed85d543e33a`.dm_dap.lu_bank
						WHERE 1=1
	--					AND date_pr = '20220221' 
		)
	,	tbl_date_pr AS (SELECT  date_pr_format date_pr
								, ROW_NUMBER() OVER (ORDER BY date_pr_format DESC) rownumber
						FROM `prj-7810ed85d543e33a`.dm_dap.dm_calendar
						WHERE 1=1
	--					AND date_pr_format >= '20220101'
	--					AND date_pr_format < FORMAT_DATE('%Y%m%d', CURRENT_DATE()) 
						AND eom_flag = 1
						GROUP BY date_pr_format
		) 
	,	acc AS (SELECT date_pr
					, trim(cod_acct_no) cod_acct_no
					, cif
				FROM `prj-7810ed85d543e33a`.dm_dap.dm_casa_funding
				WHERE 1=1 
	--			AND date_pr IN (SELECT date_pr
	--							FROM tbl_date_pr
	--							WHERE 1=1 
	--							AND rownumber = 1)
		)			
	,	cust AS (SELECT date_pr
						, cod_cust_id cif
						, nam_cust_full
				FROM `prj-7810ed85d543e33a`.misplus.bd_ci_custmast
				WHERE 1=1 
	--			 AND date_pr IN (SELECT date_pr
	--			 			     FROM tbl_date_pr
	--			 			     WHERE 1=1
	--			 			     AND rownumber = 1)
				AND cod_cust_id IN (SELECT cif
									FROM acc)
		)	
	,  cb_in AS (SELECT date_pr
						, trim(cod_acct_no) cod_acct_no
						, dat_txn trx_timestamp
						, FORMAT_DATE('%Y%m%d', dat_txn) trx_date
						, substr(txt_txn_desc, 8, 8) bankcode_ctparty
						, substr(txt_txn_desc, 17, LENGTH(txt_txn_desc) - 17) nam_ctparty
						, cod_drcr
						, trim(ref_txn_no) ref_txn_no
						, amt_txn_tcy
						, substr(ref_usr_no, 1, LENGTH(trim(ref_usr_no)) - 2) ref_usr_no
				FROM `prj-7810ed85d543e33a`.misplus.bd_ch_nobook
				WHERE 1=1
				--AND date_pr IN (SELECT date_pr
								--FROM tbl_date_pr
									--WHERE 1=1 
									--AND rownumber <= 6)
					--AND trim(cod_acct_no) IN (SELECT cod_acct_no
													--FROM acc)
				AND cod_txn_mnemonic IN (2627)
				AND cod_drcr = 'C'
				AND txt_txn_desc LIKE 'BIFAST%'
		)
	,	api_in AS (SELECT trim(user_ref_no) ref_usr_no
						, SPLIT(id_3, '|')[SAFE_OFFSET(1)] cod_acct_no_ctparty
						, date_pr
				FROM `prj-7810ed85d543e33a`.misplus.bd_api_txn_log
				WHERE 1=1 
	--             AND date_pr >= (SELECT date_pr
	--             			     FROM tbl_date_pr
	--             			     WHERE 1=1 
	--             			     AND rownumber = 7)
	--             AND date_pr <= (SELECT date_pr
	--             			     FROM tbl_date_pr
	--             			     WHERE 1=1
	--             			     AND rownumber = 1)
				AND substr(service_code, 1, 6) = 'BIFAST'
				AND response_code = '000000'
				AND trim(service_code) IN ('BIFAST_CT_INCOMING')
				AND trim(user_ref_no) IN (SELECT ref_usr_no
										FROM cb_in)
		) -- table belum ada
	,  cb_out AS (SELECT date_pr
						, trim(cod_acct_no) cod_acct_no
						, dat_txn trx_timestamp
						, FORMAT_DATE('%Y%m%d', dat_txn) trx_date
						, cod_drcr
						, trim(ref_txn_no) ref_txn_no
						, amt_txn_tcy
						, trim(ref_usr_no) ref_usr_no
						, trim(substr(ref_usr_no, 9, LENGTH(ref_usr_no))) ref_usr_no2
				FROM `prj-7810ed85d543e33a`.misplus.bd_ch_nobook
				WHERE 1=1
	--			  AND date_pr IN (SELECT date_pr
	--			  				  FROM tbl_date_pr
	--			  				  WHERE 1=1 
	--			  				  AND rownumber <= 6)
				AND COD_TXN_MNEMONIC IN (2382)
				AND COD_DRCR = 'D'
	--			  AND trim(cod_acct_no) IN (SELECT cod_acct_no 
	--					   					FROM acc)
	)
	,	xfol_out AS (SELECT contra_acct_no cod_acct_no_ctparty
							, substr(SPLIT(txt_additional_data, '000011')[SAFE_OFFSET(0)], 28, LENGTH(SPLIT(txt_additional_data,'000011')[SAFE_OFFSET(0)]) - 27) nam_ctparty
							, trim(cod_acct_no) cod_acct_no
							, trim(ref_txn_no) ref_txn_no
							, trim(ref_usr_no) ref_usr_no
							, date_pr
					FROM `prj-7810ed85d543e33a`.misplus.bd_xf_ol_st_cotxn_mmdd
					WHERE 1=1 
	--				 AND date_pr IN (SELECT date_pr
	--				    			 FROM tbl_date_pr
	--			 		         	 WHERE 1=1 
	--                              AND rownumber <= 6) 
					AND COD_TXN_MNEMONIC IN (2382)
					AND flg_drcr = 'D'
	--				 AND trim(ref_usr_no) IN (SELECT ref_usr_no
	--			           					   FROM cb_out)
	--				 AND trim(cod_acct_no) IN (SELECT cod_acct_no
	--										   FROM cb_out)
		)
	,	api_out AS (SELECT trim(id_10) ref_usr_no2
						, SPLIT(id_8, '|')[SAFE_OFFSET(0)] bankcode_ctparty
						, date_pr
					FROM `prj-7810ed85d543e33a`.misplus.bd_api_txn_log
					WHERE 1=1 
	--              AND date_pr >= (SELECT date_pr
	--								FROM tbl_date_pr
	--								WHERE 1=1
	--								AND rownumber = 7)
	--				AND date_pr <= (SELECT date_pr
	--							    FROM tbl_date_pr
	--							    WHERE 1=1
	--							    AND rownumber = 1)
					AND substr(service_code, 1, 6) = 'BIFAST'
					AND response_code = '000000'
					AND trim(service_code) IN ('BIFAST_CREDIT_TF')
	--				AND trim(id_10) IN (SELECT ref_usr_no2
	--									  FROM cb_out)
		) -- TABLE BELUM ADA
	SELECT cif
		, nam_cust_full 
		, trx_type
		, trx_inout
		, trx_ccy
		, nam_ccy_short 
		, 'IDN' AS country_code -- TIDAK ADA DI QUERY
		, nam_ctparty
		, cod_acct_no_ctparty
		, bank_ctparty
		, COUNT(*) sum_not_trx 
		, SUM(trx_amountidr) sum_trx_amountidr
		, NULL flag_to_self 
		, NULL flag_have_danamon
		, NULL cif_bdi_ctparty
	FROM 	
	(
	SELECT acc.cif
		, acc.cod_acct_no
		, cust.nam_cust_full
		, cb_in.trx_timestamp
		, cb_in.trx_date
		, 'DIGITAL' trx_channel
		, 'BI FAST' trx_type
		, 'IN' trx_inout
		, 840 trx_ccy
		, 'IDR' nam_ccy_short
		, cb_in.amt_txn_tcy trx_amountccy
		, 1 trx_ccyrate
		, cb_in.amt_txn_tcy trx_amountidr
		, api_in.cod_acct_no_ctparty
		, cb_in.nam_ctparty
		, bank_code.nam_bank_alias bank_ctparty
		, cb_in.ref_txn_no
		, cb_in.date_pr
	FROM cb_in
	LEFT OUTER JOIN acc ON cb_in.cod_acct_no = acc.cod_acct_no
	LEFT OUTER JOIN cust ON acc.cif = cust.cif
	LEFT OUTER JOIN api_in ON cb_in.ref_usr_no = api_in.ref_usr_no
	LEFT OUTER JOIN bank_code ON cb_in.bankcode_ctparty = bank_code.cod_sw_bank
	UNION ALL 
	SELECT acc.cif
		, acc.cod_acct_no
		, cust.nam_cust_full
		, cb_out.trx_timestamp
		, cb_out.trx_date
		, 'DIGITAL' trx_channel
		, 'BI FAST' trx_type
		, 'OUT' trx_inout
		, 840 trx_ccy
		, 'IDR' nam_ccy_short
		, cb_out.amt_txn_tcy trx_amountccy
		, 1 trx_ccyrate
		, cb_out.amt_txn_tcy trx_amountidr
		, xfol_out.cod_acct_no_ctparty
		, xfol_out.nam_ctparty
		, bank_code.nam_bank_alias bank_ctparty
		, cb_out.ref_txn_no
		, cb_out.date_pr
	FROM cb_out
	LEFT OUTER JOIN acc ON cb_out.cod_acct_no = acc.cod_acct_no
	LEFT OUTER JOIN cust ON acc.cif = cust.cif
	LEFT OUTER JOIN xfol_out ON cb_out.ref_txn_no = xfol_out.ref_txn_no
							AND cb_out.cod_acct_no = xfol_out.cod_acct_no
	LEFT OUTER JOIN api_out ON cb_out.ref_usr_no2 = api_out.ref_usr_no2
	LEFT OUTER JOIN bank_code ON api_out.bankcode_ctparty = bank_code.cod_sw_bank) aa
	GROUP BY cif
		, nam_cust_full 
		, trx_type
		, trx_inout
		, trx_ccy
		, nam_ccy_short 
		, country_code
		, nam_ctparty
		, cod_acct_no_ctparty
		, bank_ctparty
	'''
	;

   --==========================trf_online======================================
   EXECUTE IMMEDIATE '''
	CREATE TEMPORARY TABLE trf_online AS 
	WITH bank_code AS (SELECT lpad(CAST(cod_bank AS STRING), 6, '0') bank_code
							, nam_bank_alias
						FROM `prj-7810ed85d543e33a`.dm_dap.lu_bank
						WHERE 1=1 
	--					AND date_pr = '20230308'
		)
	, tbl_date_pr AS ( SELECT date_pr_format date_pr
							, row_number() OVER (ORDER BY date_pr_format DESC) rownumber
						FROM `prj-7810ed85d543e33a`.dm_dap.dm_calendar
						WHERE 1=1 
	--					AND date_pr_format >= '20220101'
	--					AND date_pr_format < FORMAT_DATE('%Y%m%d', CURRENT_DATE())
						AND eom_flag = 1
						GROUP BY date_pr_format
		)
	, acc AS (SELECT date_pr
					, trim(cod_acct_no) cod_acct_no
					, cif
				FROM `prj-7810ed85d543e33a`.dm_dap.dm_casa_funding
				WHERE 1=1 
	--			AND date_pr IN (SELECT date_pr
	--							FROM tbl_date_pr
	--							WHERE 1=1 
	--							AND rownumber = 1)
		)
	, cust AS (SELECT date_pr
					, cod_cust_id cif
					, nam_cust_full
				FROM `prj-7810ed85d543e33a`.misplus.bd_ci_custmast
				WHERE 1=1 
	--			AND date_pr IN (SELECT date_pr
	--							FROM tbl_date_pr
	--							WHERE 1=1 
	--							AND rownumber = 1)
				AND cod_cust_id IN (SELECT cif
									FROM acc)
		)
	, cb_in AS (SELECT  date_pr
						, trim(cod_acct_no) cod_acct_no
						, dat_txn trx_timestamp
						, FORMAT_DATE('%Y%m%d', dat_txn) trx_date
						, cod_drcr
						, trim(ref_txn_no) ref_txn_no
						, amt_txn_tcy
						, trim(ref_usr_no) ref_usr_no
				FROM `prj-7810ed85d543e33a`.misplus.bd_ch_nobook
				WHERE 1=1 
	--			AND date_pr IN (SELECT date_pr
	--							FROM tbl_date_pr
	--							WHERE 1=1 
	--							AND rownumber <= 6)
				AND COD_TXN_MNEMONIC IN (2227, 2327)
				AND cod_drcr = 'C'
				AND trim(cod_acct_no) IN (SELECT cod_acct_no
											FROM acc)
		)
	, xfol_in AS (SELECT trim(cod_acct_no) cod_acct_no
						, trim(ref_txn_no) ref_txn_no
						, trim(ref_usr_no) ref_usr_no
						, trim(contra_acct_no) cod_acct_no_ctparty
						, trim(substr(txt_additional_data, 9, 20)) nam_ctparty
						, substr(txt_additional_data, 41, 6) bankcode_ctparty
						, date_pr
					FROM `prj-7810ed85d543e33a`.misplus.bd_xf_ol_st_cotxn_mmdd
					WHERE 1=1 
	--				AND date_pr IN (SELECT date_pr
	--								  FROM tbl_date_pr
	--								  WHERE 1=1 
	--								  AND rownumber <= 6)
					AND trim(ref_usr_no) IN (SELECT ref_usr_no
											FROM cb_in)
					AND trim(cod_acct_no) IN (SELECT cod_acct_no
												FROM cb_in)
		)
	, cb_out AS (SELECT date_pr
						, trim(cod_acct_no) cod_acct_no
						, dat_txn trx_timestamp
						, FORMAT_DATE('%Y%m%d', dat_txn) trx_date
						, cod_txn_mnemonic
						, cod_drcr
						, trim(ref_txn_no) ref_txn_no
						, amt_txn_tcy
						, trim(ref_usr_no) ref_usr_no
				FROM `prj-7810ed85d543e33a`.misplus.bd_ch_nobook
				WHERE 1=1 
	--			AND date_pr IN (SELECT date_pr
	--							FROM tbl_date_pr
	--							WHERE 1=1 
	--							and rownumber <= 6)
				AND COD_TXN_MNEMONIC IN (2271,2273,2295,2297,2311,2315)
				AND cod_drcr = 'D'
				AND trim(cod_acct_no) IN (SELECT cod_acct_no
											FROM acc)
		)
	, xfol_out AS (SELECT trim(cod_acct_no) cod_acct_no
						, trim(ref_txn_no) ref_txn_no
						, trim(ref_usr_no) ref_usr_no
						, trim(contra_acct_no) cod_acct_no_ctparty
						, trim(substr(SPLIT(txt_additional_data, '000011')[SAFE_OFFSET(0)], 29, char_length(txt_additional_data) - 29)) nam_ctparty
						, substr(SPLIT(txt_additional_data, '000011')[SAFE_OFFSET(1)], 1,6) bankcode_ctparty
						, cod_txn_mnemonic
						, date_pr
					FROM `prj-7810ed85d543e33a`.misplus.bd_xf_ol_st_cotxn_mmdd
					WHERE 1=1
	--				AND date_pr IN (SELECT date_pr
	--								FROM tbl_date_pr
	--								WHERE 1=1
	--								AND rownumber <= 6)
					AND trim(ref_usr_no) IN (SELECT ref_usr_no
											FROM cb_out)
					AND trim(cod_acct_no) IN (SELECT cod_acct_no
												FROM cb_out)
		) 
	SELECT cif
		, nam_cust_full 
		, trx_type
		, trx_inout
		, trx_ccy
		, nam_ccy_short 
		, 'IDN' AS country_code -- TIDAK ADA DI QUERY
		, nam_ctparty
		, cod_acct_no_ctparty
		, bank_ctparty
		, COUNT(*) sum_not_trx 
		, SUM(trx_amountidr) sum_trx_amountidr
		, NULL flag_to_self 
		, NULL flag_have_danamon
		, NULL cif_bdi_ctparty
	FROM 	
	(
	SELECT acc.cif
		, acc.cod_acct_no
		, cust.nam_cust_full
		, cb_in.trx_timestamp
		, cb_in.trx_date
		, '' trx_channel
		, 'ONLINE' trx_type
		, 'IN' trx_inout
		, 360 trx_ccy
		, 'IDR' nam_ccy_short
		, cb_in.amt_txn_tcy trx_amountccy
		, 1 trx_ccyrate
		, cb_in.amt_txn_tcy trx_amountidr
		, xfol_in.cod_acct_no_ctparty
		, xfol_in.nam_ctparty
		, bank_code.nam_bank_alias bank_ctparty
		, cb_in.ref_txn_no
		, cb_in.date_pr
	FROM cb_in
	LEFT OUTER JOIN acc ON cb_in.cod_acct_no = acc.cod_acct_no
	LEFT OUTER JOIN cust ON acc.cif = cust.cif
	LEFT OUTER JOIN xfol_in ON cb_in.ref_txn_no = xfol_in.ref_txn_no
						AND cb_in.cod_acct_no = xfol_in.cod_acct_no
	LEFT OUTER JOIN bank_code ON xfol_in.bankcode_ctparty = bank_code.bank_code
	UNION ALL 
	SELECT acc.cif
		, acc.cod_acct_no
		, cust.nam_cust_full
		, cb_out.trx_timestamp
		, cb_out.trx_date
		, '' trx_channel
		, 'ONLINE' trx_type
		, 'OUT' trx_inout
		, 360 trx_ccy
		, 'IDR' nam_ccy_short
		, cb_out.amt_txn_tcy trx_amountccy
		, 1 trx_ccyrate
		, cb_out.amt_txn_tcy trx_amountidr
		, xfol_out.cod_acct_no_ctparty
		, xfol_out.nam_ctparty
		, bank_code.nam_bank_alias bank_ctparty
		, cb_out.ref_txn_no
		, cb_out.date_pr
	FROM cb_out
	LEFT OUTER JOIN acc ON cb_out.cod_acct_no = acc.cod_acct_no
	LEFT OUTER JOIN cust ON acc.cif = cust.cif
	LEFT OUTER JOIN xfol_out ON cb_out.ref_txn_no = xfol_out.ref_txn_no
							AND cb_out.cod_acct_no = xfol_out.cod_acct_no
							AND cb_out.cod_txn_mnemonic = xfol_out.cod_txn_mnemonic
	LEFT OUTER JOIN bank_code ON xfol_out.bankcode_ctparty = bank_code.bank_code
	) aa
	GROUP BY cif
		, nam_cust_full 
		, trx_type
		, trx_inout
		, trx_ccy
		, nam_ccy_short 
		, country_code
		, nam_ctparty
		, cod_acct_no_ctparty
		, bank_ctparty
	'''
	;

   --==========================trf_overbooking======================================
   EXECUTE IMMEDIATE '''
   CREATE TEMPORARY TABLE trf_overbooking AS
   WITH bank_code AS (SELECT lpad(CAST(cod_bank AS STRING), 6, '0') bank_code
                           , nam_bank_alias 
                        FROM `prj-7810ed85d543e33a`.dm_dap.lu_bank
                        WHERE 1=1
   --                     AND date_pr = '20230308'
      )
   ,  tbl_date_pr AS (SELECT date_pr_format date_pr
                           , row_number() OVER (ORDER BY date_pr_format DESC) rownumber 
                     FROM `prj-7810ed85d543e33a`.dm_dap.dm_calendar
                     WHERE 1=1
   --                  AND date_pr_format >= '20220101' 
   --                  AND date_pr_format < FORMAT_DATE('%Y%m%d', CURRENT_DATE()) 
                     AND eom_flag = 1 
                     GROUP BY date_pr_format
      )
   ,  acc AS (SELECT date_pr
                  , trim(cod_acct_no) cod_acct_no
                  , cif 
            FROM `prj-7810ed85d543e33a`.dm_dap.dm_casa_funding
   --         WHERE date_pr IN (SELECT date_pr 
   --                           FROM tbl_date_pr 
   --                           WHERE rownumber = 1)
      )
   ,  acc2 AS (SELECT date_pr
                     , trim(cod_acct_no) cod_acct_no
                     , cif 
               FROM `prj-7810ed85d543e33a`.dm_dap.dm_casa_funding
   --            WHERE date_pr IN (SELECT date_pr 
   --                              FROM tbl_date_pr 
   --                              WHERE rownumber = 1)
      )
   ,  cust AS (SELECT date_pr
                     , cod_cust_id cif
                     , nam_cust_full 
               FROM `prj-7810ed85d543e33a`.misplus.bd_ci_custmast
               WHERE 1=1 
   --            AND date_pr IN (SELECT date_pr 
   --                              FROM tbl_date_pr 
   --                              WHERE rownumber = 1) 
               AND cod_cust_id IN (SELECT cif 
                                    FROM acc)
      )
   ,  cust2 AS (SELECT date_pr
                     , cod_cust_id cif
                     , nam_cust_full 
               FROM `prj-7810ed85d543e33a`.misplus.bd_ci_custmast
               WHERE 1=1  
   --            AND date_pr IN (SELECT date_pr 
   --                              FROM tbl_date_pr 
   --                              WHERE rownumber = 1) 
               AND cod_cust_id IN (SELECT cif 
                                 FROM acc)
      )
   ,  trf1 AS (SELECT date_pr
                     , trim(cod_acct_no) cod_acct_no
                     , dat_txn trx_timestamp
                     , FORMAT_DATE('%Y%m%d', dat_txn) trx_date
                     , cod_drcr
                     , '000011' bankcode_ctparty
                     , trim(ref_txn_no) ref_txn_no
                     , amt_txn_tcy
                     , trim(ref_usr_no) ref_usr_no 
               FROM `prj-7810ed85d543e33a`.misplus.bd_ch_nobook
               WHERE 1=1
   --            AND date_pr IN (SELECT date_pr 
   --                              FROM tbl_date_pr 
   --                              WHERE rownumber <= 6) 
               AND COD_TXN_MNEMONIC IN (1006,1015,1320,1702,1710,2204,2205,2269,2289,2305,2647
                                       ,2654,2640,2670,7606,7607,2212,1703,1704,9910,9911,9991) 
               AND trim(cod_acct_no) IN (SELECT cod_acct_no 
                                          FROM acc)
      )
   , trf2 AS (SELECT date_pr
                     , trim(cod_acct_no) cod_acct_no
                     , dat_txn trx_timestamp
                     , FORMAT_DATE('%Y%m%d', dat_txn) trx_date
                     , cod_drcr
                     , trim(ref_txn_no) ref_txn_no
                     , amt_txn_tcy
                     , trim(ref_usr_no) ref_usr_no 
               FROM `prj-7810ed85d543e33a`.misplus.bd_ch_nobook
               WHERE 1=1
   --            AND date_pr IN (SELECT date_pr 
   --                            FROM tbl_date_pr 
   --                            WHERE rownumber <= 6) 
               AND COD_TXN_MNEMONIC IN (1006,1015,1320,1702,1710,2204,2205,2269,2289,2305,2647
                                       ,2654,2640,2670,7606,7607,2212,1703,1704,9910,9911,9991) 
               AND trim(cod_acct_no) IN (SELECT cod_acct_no 
                                          FROM acc)
      ) 
   SELECT cif
         , nam_cust_full 
         , trx_type
         , trx_inout
         , trx_ccy
         , nam_ccy_short 
         , 'IDN' AS country_code -- TIDAK ADA DI QUERY
         , nam_ctparty
         , cod_acct_no_ctparty
         , bank_ctparty
         , COUNT(*) sum_not_trx 
         , SUM(trx_amountidr) sum_trx_amountidr
         , NULL flag_to_self 
         , NULL flag_have_danamon
         , NULL cif_bdi_ctparty
   FROM 	
   (
   SELECT acc.cif
         , acc.cod_acct_no
         , cust.nam_cust_full
         , trf1.trx_timestamp
         , trf1.trx_date
         , '' trx_channel
         , 'ONLINE' trx_type
         , CASE 
            WHEN trf1.COD_DRCR = 'C' THEN 'IN' 
            ELSE 'OUT' 
            END trx_inout
         , 360 trx_ccy
         , 'IDR' nam_ccy_short
         , trf1.amt_txn_tcy trx_amountccy
         , 1 trx_ccyrate
         , trf1.amt_txn_tcy trx_amountidr
         , trf2.cod_acct_no cod_acct_no_ctparty
         , cust2.nam_cust_full nam_ctparty
         , bank_code.nam_bank_alias bank_ctparty
         , trf1.ref_txn_no
         , trf1.date_pr 
   FROM trf1 
   LEFT OUTER JOIN acc ON trf1.cod_acct_no = acc.cod_acct_no 
   LEFT OUTER JOIN cust ON acc.cif = cust.cif 
   INNER JOIN trf2 ON trf1.date_pr = trf2.date_pr 
                  AND trf1.ref_txn_no = trf2.ref_txn_no 
                  AND trf1.amt_txn_tcy = trf2.amt_txn_tcy 
                  AND trf1.COD_DRCR != trf2.COD_DRCR 
                  AND trf1.COD_ACCT_NO != trf2.COD_ACCT_NO 
   LEFT OUTER JOIN acc2 ON trf2.cod_acct_no = acc2.cod_acct_no 
   LEFT OUTER JOIN cust2 ON acc2.cif = cust2.cif 
   LEFT OUTER JOIN bank_code ON trf1.bankcode_ctparty = bank_code.bank_code
   ) aa
   GROUP BY cif
         , nam_cust_full 
         , trx_type
         , trx_inout
         , trx_ccy
         , nam_ccy_short 
         , country_code
         , nam_ctparty
         , cod_acct_no_ctparty
         , bank_ctparty
   '''
   ;

   --==========================trf_rtgs======================================
   EXECUTE IMMEDIATE '''
   CREATE TEMPORARY TABLE trf_rtgs AS
   WITH bank_code AS (SELECT lpad(CAST(cod_bank AS STRING), 3, '0') bank_code
                           , nam_bank_alias 
                        FROM `prj-7810ed85d543e33a`.dm_dap.lu_bank
                        WHERE 1=1
   --                     AND date_pr = '20230308'
      )
   ,  tbl_date_pr AS (SELECT date_pr_format date_pr
                           , row_number() OVER (ORDER BY date_pr_format DESC) rownumber 
                        FROM `prj-7810ed85d543e33a`.dm_dap.dm_calendar
                        WHERE 1=1
   --                     AND date_pr_format >= '20220101' 
   --                     AND date_pr_format < FORMAT_DATE('%Y%m%d', CURRENT_DATE()) 
                        AND eom_flag = 1 
                        GROUP BY date_pr_format
      )
   ,  trx1 AS (SELECT cod_cust_id
                     , ref_subseq_no
                     , trim(cod_acct_no) cod_acct_no
                     , SPLIT(cod_payment_txn, '_')[SAFE_OFFSET(0)] trx_channel
                     , cod_network_id
                     , 'RTGS' trx_type
                     , CASE 
                           WHEN substr(trim(cod_network_id), length(trim(cod_network_id)), 1) = 'I' THEN 'IN' 
                           ELSE 'OUT' 
                     END trx_inout
                     , cod_txn_ccy trx_ccy
                     , amt_txn_tcy trx_amountccy
                     , rat_conv_tclcy trx_ccyrate
                     , amt_txn_lcy trx_amountidr
                     , trim(ref_txn_no) ref_txn_no
                     , dat_txn trx_timestamp
                     , FORMAT_DATE('%Y%m%d', dat_txn) trx_date
                     , date_pr 
               FROM `prj-7810ed85d543e33a`.misplus.bd_pm_txn_log
               WHERE 1=1
   --            AND date_pr IN (SELECT date_pr 
   --                              FROM tbl_date_pr 
   --                              WHERE rownumber <= 6) 
               AND FORMAT_DATE('%Y%m', dat_txn) = substr(date_pr, 1, 6) 
               AND REGEXP_CONTAINS(cod_network_id, r'^RTGS')
      )
   ,  trx2 AS (SELECT cod_ctparty_acct
                     , ref_subseq_no
                     , upper(nam_ctparty) nam_ctparty
                     , substr(cod_ctparty_routing, 1, 3) cod_bi
                     , trim(ref_txn_no) ref_txn_no
                     , date_pr 
               FROM `prj-7810ed85d543e33a`.misplus.bd_pm_rel_txn_store
               WHERE 1=1
   --            AND date_pr IN (SELECT date_pr 
   --                              FROM tbl_date_pr 
   --                              WHERE rownumber <= 6) 
               AND trim(ref_txn_no) IN (SELECT ref_txn_no 
                                       FROM trx1)
      )
   ,  trx3 AS (SELECT trim(ref_txn_no) ref_txn_no
                     , ref_sub_seq_no
                     , txt_txn_desc
                     , date_pr 
               FROM `prj-7810ed85d543e33a`.misplus.bd_ch_nobook
               WHERE 1=1
   --            AND date_pr IN (SELECT date_pr 
   --                              FROM tbl_date_pr 
   --                              WHERE rownumber <= 6) 
               AND trim(ref_txn_no) IN  (SELECT ref_txn_no 
                                          FROM trx1) 
               AND cod_txn_mnemonic NOT IN (5003)
      )
   ,  cust AS (SELECT cod_cust_id
                     , nam_cust_full 
               FROM `prj-7810ed85d543e33a`.misplus.bd_ci_custmast
               WHERE 1=1
   --            AND date_pr IN (SELECT date_pr 
   --                              FROM tbl_date_pr 
   --                              WHERE rownumber = 1) 
               AND cod_cust_id IN (SELECT cod_cust_id 
                                    FROM trx1)
      )
   ,  ccy AS (SELECT cod_ccy
                     , nam_ccy_short
                     , nam_currency 
               FROM `prj-7810ed85d543e33a`.misplus.bd_ba_ccy_code
               WHERE 1=1
   --            AND date_pr IN  (SELECT date_pr 
   --                                 FROM tbl_date_pr 
   --                                 WHERE rownumber = 1)
      ) 
   SELECT cif
         , nam_cust_full 
         , trx_type
         , trx_inout
         , trx_ccy
         , nam_ccy_short 
         , 'IDN' AS country_code -- TIDAK ADA DI QUERY
         , nam_ctparty
         , cod_acct_no_ctparty
         , bank_ctparty
         , COUNT(*) sum_not_trx 
         , SUM(trx_amountidr) sum_trx_amountidr
         , NULL flag_to_self 
         , NULL flag_have_danamon
         , NULL cif_bdi_ctparty
   FROM 	
   (
   SELECT trx1.cod_cust_id cif
         , trx1.cod_acct_no
         , cust.nam_cust_full
         , trx1.trx_timestamp
         , trx1.trx_date
         , trx1.trx_channel
         , trx1.trx_type
         , trx1.trx_inout
         , trx1.trx_ccy
         , ccy.nam_ccy_short
         , trx1.trx_amountccy
         , trx1.trx_ccyrate
         , trx1.trx_amountidr
         , trx2.cod_ctparty_acct cod_acct_no_ctparty
         , CASE 
               WHEN trx_type = 'RTGS' AND trx_inout = 'IN' THEN upper(SPLIT(trx3.txt_txn_desc, '_')[SAFE_OFFSET(3)]) 
               ELSE upper(trx2.nam_ctparty) 
            END nam_ctparty
         , CASE 
               WHEN trx_type = 'RTGS' AND trx_inout = 'IN' THEN trx2.nam_ctparty 
               ELSE bank_code.nam_bank_alias 
            END bank_ctparty
         , trx1.ref_txn_no
         , trx1.date_pr 
   FROM trx1 
   LEFT OUTER JOIN trx2 ON trx1.date_pr = trx2.date_pr 
                     AND trx1.ref_txn_no = trx2.ref_txn_no 
                     AND trx1.ref_subseq_no = trx2.ref_subseq_no 
   LEFT OUTER JOIN trx3 ON trx1.date_pr = trx3.date_pr 
                     AND trx1.ref_txn_no = trx3.ref_txn_no 
                     AND trx1.trx_inout = 'IN' 
   LEFT OUTER JOIN cust ON trx1.cod_cust_id = cust.cod_cust_id 
   LEFT OUTER JOIN bank_code ON trx2.cod_bi = bank_code.bank_code 
   LEFT OUTER JOIN ccy ON trx1.trx_ccy = ccy.cod_ccy  
   ) aa
   GROUP BY cif
         , nam_cust_full 
         , trx_type
         , trx_inout
         , trx_ccy
         , nam_ccy_short 
         , country_code
         , nam_ctparty
         , cod_acct_no_ctparty
         , bank_ctparty
   '''
   ;

   --==========================trf_skn======================================
   EXECUTE IMMEDIATE '''
   CREATE TEMPORARY TABLE trf_skn AS 
   WITH bank_code AS (SELECT lpad(CAST(cod_bank AS STRING), 3, '0') bank_code
                           , nam_bank_alias 
                        FROM `prj-7810ed85d543e33a`.dm_dap.lu_bank
                        WHERE 1=1
   --                     AND date_pr = '20230308'
      )
   ,  tbl_date_pr AS (SELECT date_pr_format date_pr
                           , row_number() OVER (ORDER BY date_pr_format DESC) rownumber 
                        FROM `prj-7810ed85d543e33a`.dm_dap.dm_calendar
                        WHERE 1=1
   --                     AND date_pr_format >= '20220101' 
   --                     AND date_pr_format < FORMAT_DATE('%Y%m%d', CURRENT_DATE()) 
                        AND eom_flag = 1 
                        GROUP BY date_pr_format
      )
   ,  trx1 AS (SELECT cod_cust_id cif
                     , ref_subseq_no
                     , trim(cod_acct_no) cod_acct_no
                     , SPLIT(cod_payment_txn, '_') [SAFE_OFFSET(0)] trx_channel
                     , cod_network_id
                     , 'SKN' trx_type
                     , CASE 
                           WHEN substr(trim(cod_network_id), length(trim(cod_network_id)), 1) = 'I' THEN 'IN' 
                           ELSE 'OUT' 
                        END trx_inout
                     , cod_txn_ccy trx_ccy
                     , amt_txn_tcy trx_amountccy
                     , rat_conv_tclcy trx_ccyrate
                     , amt_txn_lcy trx_amountidr
                     , trim(ref_txn_no) ref_txn_no
                     , dat_txn trx_timestamp
                     , FORMAT_DATE('%Y%m%d', dat_txn) trx_date
                     , date_pr 
               FROM `prj-7810ed85d543e33a`.misplus.bd_pm_txn_log 
               WHERE 1=1
   --            AND date_pr IN (SELECT date_pr 
   --                              FROM tbl_date_pr 
   --                              WHERE rownumber <= 6) 
   --            AND FORMAT_DATE('%Y%m', dat_txn) = substr(date_pr, 1, 6) 
               AND REGEXP_CONTAINS(cod_network_id, r'^SKN')
      )
   ,  trx2 AS (SELECT cod_ctparty_acct
                     , ref_subseq_no
                     , upper(nam_ctparty) nam_ctparty
                     , substr(cod_ctparty_routing, 1, 3) cod_bi
                     , trim(ref_txn_no) ref_txn_no
                     , date_pr 
               FROM `prj-7810ed85d543e33a`.misplus.bd_pm_rel_txn_store
               WHERE 1=1
   --            AND date_pr IN (SELECT date_pr 
   --                              FROM tbl_date_pr 
   --                              WHERE rownumber <= 6) 
               AND trim(ref_txn_no) IN (SELECT ref_txn_no 
                                       FROM trx1)
      )
   ,  cif_trx AS (SELECT cif
                        , count(1) 
                  FROM trx1 
                  GROUP BY cif
      )
   ,  cust AS (SELECT cod_cust_id
                     , nam_cust_full 
               FROM `prj-7810ed85d543e33a`.misplus.bd_ci_custmast
               WHERE 1=1
   --            AND date_pr IN (SELECT date_pr 
   --                              FROM tbl_date_pr 
   --                              WHERE rownumber = 1) 
               AND cod_cust_id IN (SELECT cif 
                                    FROM cif_trx)
      )
   ,  ccy AS (SELECT cod_ccy
                     , nam_ccy_short
                     , nam_currency 
               FROM `prj-7810ed85d543e33a`.misplus.bd_ba_ccy_code
               WHERE 1=1
   --            AND date_pr IN (SELECT date_pr 
   --                              FROM tbl_date_pr 
   --                              WHERE rownumber = 1)
      ) 
   SELECT cif
         , nam_cust_full 
         , trx_type
         , trx_inout
         , trx_ccy
         , nam_ccy_short 
         , 'IDN' AS country_code -- TIDAK ADA DI QUERY
         , nam_ctparty
         , cod_acct_no_ctparty
         , bank_ctparty
         , COUNT(*) sum_not_trx 
         , SUM(trx_amountidr) sum_trx_amountidr
         , NULL flag_to_self 
         , NULL flag_have_danamon
         , NULL cif_bdi_ctparty
   FROM 	
   (
   SELECT trx1.cif
         , trx1.cod_acct_no
         , cust.nam_cust_full
         , trx1.trx_timestamp
         , trx1.trx_date
         , trx1.trx_channel
         , trx1.trx_type
         , trx1.trx_inout
         , trx1.trx_ccy
         , ccy.nam_ccy_short
         , trx1.trx_amountccy
         , trx1.trx_ccyrate
         , trx1.trx_amountidr
         , trx2.cod_ctparty_acct cod_acct_no_ctparty
         , upper(trx2.nam_ctparty) nam_ctparty
         , bank_code.nam_bank_alias bank_ctparty
         , trx1.ref_txn_no
         , trx1.date_pr 
   FROM trx1 
   LEFT OUTER JOIN trx2 ON trx1.date_pr = trx2.date_pr 
                     AND trx1.ref_txn_no = trx2.ref_txn_no 
                     AND trx1.ref_subseq_no = trx2.ref_subseq_no 
   LEFT OUTER JOIN cust ON trx1.cif = cust.cod_cust_id 
   LEFT OUTER JOIN bank_code ON trx2.cod_bi = bank_code.bank_code 
   LEFT OUTER JOIN ccy ON trx1.trx_ccy = ccy.cod_ccy
   ) aa
   GROUP BY cif
         , nam_cust_full 
         , trx_type
         , trx_inout
         , trx_ccy
         , nam_ccy_short 
         , country_code
         , nam_ctparty
         , cod_acct_no_ctparty
         , bank_ctparty
   '''
   ;

   --==========================trf_swift======================================
   EXECUTE IMMEDIATE '''
   CREATE TEMPORARY TABLE trf_swift AS 
   WITH bank_code AS (SELECT lpad(CAST(cod_bank AS STRING), 3, '0') bank_code
                           , nam_bank_alias 
                  FROM `prj-7810ed85d543e33a`.dm_dap.lu_bank
                  WHERE 1=1
--                   AND date_pr = '20230308'
   )
   , tbl_date_pr AS (SELECT date_pr_format date_pr
                        , row_number() OVER (ORDER BY date_pr_format DESC) rownumber 
                  FROM `prj-7810ed85d543e33a`.dm_dap.dm_calendar
                  WHERE 1=1
--                  AND date_pr_format >= '20220101' 
--                  AND date_pr_format <= FORMAT_DATE('%Y%m%d', CURRENT_DATE()) 
                  AND eom_flag = 1 
                  GROUP BY date_pr_format
   )
   , trx1 AS (SELECT cod_cust_id
                  , trim(cod_acct_no) cod_acct_no
                  , SPLIT(cod_payment_txn, '_')[SAFE_OFFSET(0)] trx_channel
                  , cod_network_id
                  , 'SWIFT' trx_type
                  , CASE 
                        WHEN substr(trim(cod_network_id), length(trim(cod_network_id)), 1) = 'I' THEN 'IN' 
                        ELSE 'OUT' 
                     END trx_inout
                  , cod_txn_ccy trx_ccy
                  , amt_txn_tcy trx_amountccy
                  , rat_conv_tclcy trx_ccyrate
                  , amt_txn_lcy trx_amountidr
                  , trim(ref_txn_no) ref_txn_no
                  , dat_txn trx_timestamp
                  , FORMAT_DATE('%Y%m%d', dat_txn) trx_date
                  , date_pr 
            FROM `prj-7810ed85d543e33a`.misplus.bd_pm_txn_log
            WHERE 1=1
--          AND date_pr IN (SELECT date_pr 
--                            FROM tbl_date_pr 
--                            WHERE rownumber <= 12) 
--          AND FORMAT_DATE('%Y%m', dat_txn) = substr(date_pr, 1, 6) 
            AND REGEXP_CONTAINS(cod_network_id, r'^SWIFT')
   )
, trx2 AS (SELECT cod_ctparty_acct
                  , ref_sw_swift_no
                  , upper(nam_ctparty) nam_ctparty
                  , cod_ctparty_country
                  , cod_sw_bic_receiver
                  , cod_sw_bic_sender
                  , trim(ref_txn_no) ref_txn_no
                  , date_pr 
            FROM `prj-7810ed85d543e33a`.misplus.bd_pm_rel_txn_store
            WHERE 1=1
--           AND date_pr IN (SELECT date_pr 
--                             FROM tbl_date_pr 
--                             WHERE rownumber <= 12) 
            AND trim(ref_txn_no) IN (SELECT ref_txn_no 
                                    FROM trx1)
   )
, trx3 AS (SELECT trim(ref_txn_no) ref_txn_no
                  , txt_txn_desc
                  , date_pr 
            FROM `prj-7810ed85d543e33a`.misplus.bd_ch_nobook
            WHERE 1=1
--           AND date_pr IN (SELECT date_pr 
--                             FROM tbl_date_pr 
--                             WHERE rownumber <= 12) 
            AND trim(ref_txn_no) IN (SELECT ref_txn_no 
                                       FROM trx1) 
            AND cod_txn_mnemonic NOT IN (5003)
   )
, cust AS (SELECT cod_cust_id
                  , nam_cust_full 
            FROM `prj-7810ed85d543e33a`.misplus.bd_ci_custmast
            WHERE 1=1
--           AND date_pr IN (SELECT date_pr 
--                             FROM tbl_date_pr 
--                             WHERE rownumber = 1) 
            AND cod_cust_id IN (SELECT cod_cust_id 
                                 FROM trx1)
   )
, ccy AS (SELECT cod_ccy
                  , nam_ccy_short
                  , nam_currency
            FROM `prj-7810ed85d543e33a`.misplus.bd_ba_ccy_code
            WHERE 1=1
--          AND date_pr IN (SELECT date_pr 
--                            FROM tbl_date_pr 
--                            WHERE rownumber = 1)
) 
SELECT cif
      , nam_cust_full 
      , trx_type
      , trx_inout
      , trx_ccy
      , nam_ccy_short 
      , 'IDN' AS country_code -- TIDAK ADA DI QUERY
      , nam_ctparty
      , cod_acct_no_ctparty
      , bank_ctparty
      , COUNT(*) sum_not_trx 
      , SUM(trx_amountidr) sum_trx_amountidr
      , NULL flag_to_self 
      , NULL flag_have_danamon
      , NULL cif_bdi_ctparty
FROM 	
(
SELECT trx1.cod_cust_id cif
      , trx1.cod_acct_no
      , cust.nam_cust_full
      , trx1.trx_timestamp
      , trx1.trx_date
      , trx1.trx_channel
      , trx1.trx_type
      , trx1.trx_inout
      , trx1.trx_ccy
      , ccy.nam_ccy_short
      , trx1.trx_amountccy
      , trx1.trx_ccyrate
      , trx1.trx_amountidr
      , CASE 
            WHEN trx_type = 'SWIFT' AND trx_inout = 'IN' THEN trx2.ref_sw_swift_no 
            ELSE trx2.cod_ctparty_acct 
         END cod_acct_no_ctparty
      , CASE 
            WHEN trx_type = 'SWIFT' AND trx_inout = 'IN' THEN upper(SPLIT(trx3.txt_txn_desc, '_')[SAFE_OFFSET(1)]) 
            ELSE upper(trx2.nam_ctparty) 
         END nam_ctparty
      , CASE 
            WHEN trx_type = 'SWIFT' AND trx_inout = 'IN' THEN trx2.cod_sw_bic_sender 
            ELSE substr(trx2.cod_sw_bic_receiver, 1, 8) 
         END bank_ctparty
      , CASE 
            WHEN trx_type = 'SWIFT' AND trx_inout = 'IN' THEN substr(trx2.cod_sw_bic_sender, 5, 2) 
            ELSE substr(trx2.cod_sw_bic_receiver, 5, 2) 
         END country_ctparty
      , trx1.ref_txn_no
      , trx1.date_pr 
FROM trx1 
LEFT OUTER JOIN trx2 ON trx1.date_pr = trx2.date_pr 
                     AND trx1.ref_txn_no = trx2.ref_txn_no 
LEFT OUTER JOIN trx3 ON trx1.date_pr = trx3.date_pr 
                     AND trx1.ref_txn_no = trx3.ref_txn_no 
LEFT OUTER JOIN cust ON trx1.cod_cust_id = cust.cod_cust_id 
LEFT OUTER JOIN ccy ON trx1.trx_ccy = ccy.cod_ccy
) aa
GROUP BY cif
      , nam_cust_full 
      , trx_type
      , trx_inout
      , trx_ccy
      , nam_ccy_short 
      , country_code
      , nam_ctparty
      , cod_acct_no_ctparty
      , bank_ctparty
'''
;

   EXECUTE IMMEDIATE '''
   INSERT INTO `''' || V_PROJECT_ID || '`.' || V_DATASET_ID || '.' || V_TARGET_TABLE || '''
   SELECT DISTINCT
         transfer.* 
		   , CAST(\'''' || cursor.EXECUTION_DATE || '''\' AS DATE) execution_date
		   , \'''' || V_JOB_ID || '''\' job_id
		   , PARSE_DATETIME("%Y%m%d%H%M%S", \'''' || V_JOB_ID || '''\') job_id_date_format
		   , CAST(\'''' || cursor.PROCESS_BUSINESS_DATE || '''\' AS DATE) business_date
   FROM 
      (SELECT * 
      FROM trf_bifast
      UNION ALL
      SELECT * 
      FROM trf_online
      UNION ALL
      SELECT * 
      FROM trf_overbooking
      UNION ALL
      SELECT * 
      FROM trf_rtgs
      UNION ALL
      SELECT * 
      FROM trf_skn
      UNION ALL
      SELECT * 
      FROM trf_swift
      ) transfer
   WHERE 1=1 
   '''
   ;

  --==================================================================

  --5. Update OMT RUNNING dengan Status Done
  EXECUTE IMMEDIATE '''
  UPDATE `''' || V_PROJECT_ID || '`.' || V_OMT_PROCESS_LOG_TABLE_NAME || '''
  SET STATUS="DONE" 
      , END_DATE=CURRENT_DATETIME("Asia/Jakarta") 
      , STATUS_DESCRIPTION= CASE WHEN STATUS_DESCRIPTION = 'AUTOFILL_BACKLOG_WITH_PROCESS' THEN STATUS_DESCRIPTION
                              ELSE NULL 
                            END
      -- , MAX_LAST_UPDATE_DATE = CAST(CAST(BUSINESS_DATE AS STRING FORMAT 'YYYY-MM-DD') || ' 23:59:59' AS DATETIME)
      , TRG_DATA_COUNT = (
                              SELECT COUNT(1)
                              FROM `''' || V_PROJECT_ID || '`.' || V_DATASET_ID || '.' || V_TARGET_TABLE || '''
                              WHERE 1=1
                                AND BUSINESS_DATE = \'''' || cursor.PROCESS_BUSINESS_DATE || '''\'
                            )
  WHERE 1=1 
      AND TRG_TBL_NAME = \''''|| V_PROJECT_ID || '.' || V_DATASET_ID || '.' || V_TARGET_TABLE || '''\'
      AND JOB_TYPE = \'''' || V_JOB_TYPE || '''\'
      AND JOB_NAME = \'''' || V_JOB_NAME || '''\'
      AND EXECUTION_DATE = \'''' || cursor.EXECUTION_DATE || '''\'
      AND BUSINESS_DATE = \'''' || cursor.PROCESS_BUSINESS_DATE || '''\'
      AND STATUS = "RUNNING"
      AND IFNULL(STATUS_DESCRIPTION,'') <> 'JOB_STATUS_FLAG'
  '''
  ;

END FOR;

END;