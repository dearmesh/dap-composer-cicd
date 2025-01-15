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