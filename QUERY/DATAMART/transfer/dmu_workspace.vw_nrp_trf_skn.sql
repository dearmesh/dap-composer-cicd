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