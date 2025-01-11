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