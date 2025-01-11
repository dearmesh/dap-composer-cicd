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