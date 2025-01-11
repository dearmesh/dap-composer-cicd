with acc AS (SELECT cif
                    , TRIM(cod_acct_no) as account_number
                    , prod_code
                    , prod_name 
                    , lob_code
                    , CASE WHEN lob_code IN(10) THEN 'CMM'
                        WHEN lob_code IN(11) THEN 'CMM'
                        WHEN lob_code IN(12) THEN 'Personal Banking'
                        WHEN lob_code IN(13) THEN 'Consumer Affluent'
                        WHEN lob_code IN(31) THEN 'SME' 
                        WHEN lob_code IN(32) THEN 'SEMM'
                        WHEN lob_code IN(41,42) THEN 'Commercial' 
                        WHEN lob_code IN(61) THEN 'Fin Institution'
                        WHEN lob_code IN(52) THEN 'SAM' 
                        WHEN lob_code IN(71,73) THEN 'Profesional Market'
                        WHEN lob_code IN(51) THEN 'Corp Banking' 
                        WHEN lob_code IN(53) THEN 'Joint Finance' 
                        WHEN lob_code IN(81) THEN 'Syariah'
                        WHEN lob_code IN(74) THEN 'ALCO' 
                        WHEN lob_code = 0 THEN 'BLANK' ELSE 'BLANK' END as lob_name
                    , date_pr
                FROM `prj-7810ed85d543e33a`.dm_dap.dm_casa_funding
                WHERE 1=1
                --AND date_pr = '20240930' 
                AND prod_code NOT IN (370,371,372,379)
                AND NOT (prod_code BETWEEN 615 AND 626)
            )
    , ccy AS ( SELECT cod_ccy
                      , nam_ccy_short
                      , date_pr
                FROM `prj-7810ed85d543e33a`.misplus.bd_ba_ccy_code
                WHERE 1=1 
                --AND date_pr = '20240930'
            )
    , mnemonic AS (SELECT txt_txn_desc AS txt_txn_desc_mnemonic
                          , cod_txn_mnemonic
                          , txt_base_txn_desc
                          , cod_txn_category
                          , cod_txn_mode
                          , cod_org_channel
                          , date_pr
                    FROM `prj-7810ed85d543e33a`.misplus.bd_ba_txn_mnemonic
                    WHERE 1=1
                    --AND date_pr='20240930' 
                    AND cod_txn_category='CUST' AND cod_org_channel='BRN'
                    AND cod_txn_mnemonic NOT IN (1040,1408,1431,1433,1451,1454,1455,1456,1459,1465,1708,1908)
            )
    , branch AS ( SELECT DISTINCT branch_code_original
                                  , branch_name
                                  , am_code
                                  , region
                                  , date_pr
                    FROM `prj-7810ed85d543e33a`.dm_dap.ref_oc_rb
                    WHERE 1=1
                    --AND date_pr = '20240930'
    )
    , trx AS ( SELECT 'BRANCH' AS channel_type
                      , dat_txn AS trx_timestamp
                      , DATE(dat_txn) AS trx_date 
                      , cod_userno
                      , TRIM(ref_txn_no) AS ref_no
                      , TRIM(ref_usr_no) AS ref_usr_no
                      , TRIM(cod_acct_no) AS account_number
                      , cod_auth_id
                      , cod_txn_mnemonic
                      , cod_drcr AS dr_or_cr
                      , 'Approved' AS `result` 
                      , 1 AS no_of_trx
                      , amt_txn_tcy * rat_conv_tclcy AS amount_idr
                      , amt_txn_tcy AS amount_origin
                      , cod_txn_ccy AS ccycode_origin
                      , rat_conv_tclcy AS ccy_rate
                      , txt_txn_desc AS trx_description
                      , CAST(cod_cc_brn_txn AS string) AS branch_code
                      , date_pr AS date_pr2
                FROM `prj-7810ed85d543e33a`.misplus.bd_ch_nobook
                WHERE 1=1
                --AND date_pr='20240930'
                AND CAST(cod_cc_brn_txn AS string) IN (SELECT branch_code_original FROM branch)
                AND cod_txn_mnemonic IN (SELECT cod_txn_mnemonic FROM mnemonic)
                AND TRIM(cod_auth_id) NOT LIKE 'HTV_%'
                AND NOT (cod_txn_mnemonic=1008 and TRIM(ref_usr_no) <> '2030')
                AND NOT ( upper(txt_txn_desc) LIKE '%ADM TRANSFER%' 
                          OR upper(txt_txn_desc) LIKE '%ADM TRNSFR%' 
                          OR upper(txt_txn_desc) LIKE '%ADM TRF%' 
                          OR upper(txt_txn_desc) LIKE '%FEE WAIVE%'
                          OR upper(txt_txn_desc) LIKE '%SERVICE CHARGE%'
                          OR (upper(txt_txn_desc) LIKE '%FEE%' AND cod_txn_mnemonic IN (2622,2647))
                          OR (ref_usr_no LIKE 'FEE%' AND cod_txn_mnemonic=2622)
                          OR (ref_usr_no LIKE '%REV%' AND cod_txn_mnemonic=2622 AND amt_txn<0)
                          OR (txt_txn_desc='BAGI HASIL' AND cod_drcr='C'  AND cod_txn_mnemonic=2648)
                          OR (cod_txn_mnemonic=2647 AND cod_drcr='D' AND txt_txn_desc LIKE 'PAJAK%')
                          OR (cod_txn_mnemonic=1408 AND ref_usr_no='3955' AND txt_txn_desc LIKE 'REV_PAJAK%')
                          OR ((cod_userno=3955 OR cod_cc_brn_txn=9211) AND txt_txn_desc LIKE 'REV%')
                          OR (ref_usr_no LIKE '8a42bdb%' AND txt_txn_desc LIKE '%PAJAK%')
            )
    )
    , LOG AS (SELECT date_pr
                     , TRIM(ref_txn_no) as ref_no
                     , cod_network_id
                     , cod_payment_txn
                     , count(1) 
                FROM `prj-7810ed85d543e33a`.misplus.bd_pm_txn_log
                WHERE 1=1
                --AND date_pr = '20240930'
                AND TRIM(ref_txn_no) IN (SELECT DISTINCT ref_no FROM trx) 
                GROUP BY 1,2,3,4
    )
    , stg AS ( SELECT trx.*,
                      ccy.nam_ccy_short AS ccynam_origin
                      , acc.cif
                      , acc.prod_code
                      , acc.prod_name
                      , acc.lob_code
                      , acc.lob_name
                      , mnemonic.txt_txn_desc_mnemonic
                      , mnemonic.txt_base_txn_desc
                      , mnemonic.cod_txn_category
                      , mnemonic.cod_txn_mode
                      , mnemonic.cod_org_channel
                      , log.cod_network_id
                      , cod_payment_txn
                      , CASE WHEN trx.cod_txn_mnemonic IN (1075,1321,1452,1466,1467,1471,1481,1482,2282,2329,2330) THEN 'PAYMENT PURCHASE'
                             WHEN trx.cod_txn_mnemonic IN (1401,2250,2236) AND trx.dr_or_cr='C' THEN 'CASH DEPOSIT'
                             WHEN trx.cod_txn_mnemonic IN (1453,1514,6601,6501) AND trx.dr_or_cr='C' THEN 'CHEQUE DEPOSIT'
                             WHEN trx.cod_txn_mnemonic IN (1001,1301,2201,2267,2283,2285,2301,2353) AND trx.dr_or_cr='D' THEN 'CASH WITHDRAWAL'
                             WHEN trx.cod_txn_mnemonic IN (1013,6101) AND trx.dr_or_cr='D' THEN 'CHEQUE WITHDRAWAL'
                             WHEN trx.cod_txn_mnemonic IN (1006,1015,1320,1321,1702,1710,2204,2205,2269,2289,2305,2647,2654,2640,2670,7606,7607,2212,1703,1704) THEN 'TRF OVERBOOKING'
                             WHEN trx.cod_txn_mnemonic IN (2271,2273,2295,2297,2311,2315) AND trx.dr_or_cr='D' THEN 'TRF OUT ONLINE'
                             WHEN trx.cod_txn_mnemonic IN (2382) AND trx.dr_or_cr='D' THEN 'TRF OUT BIFAST'
                             WHEN trx.cod_txn_mnemonic IN (2622) AND substr(upper(TRIM(trx.trx_description)),1,6)='BIFAST' AND trx.dr_or_cr='D' THEN 'TRF OUT BIFAST'
                             WHEN trx.cod_txn_mnemonic IN (1008) AND trx.dr_or_cr='D' AND log.cod_payment_txn LIKE '%SKN%'  THEN 'TRF OUT SKN'
                             WHEN trx.cod_txn_mnemonic IN (1008) AND trx.dr_or_cr='D' AND log.cod_payment_txn LIKE '%RTGS%' THEN 'TRF OUT RTGS'
                             WHEN trx.cod_txn_mnemonic IN (1008) AND trx.dr_or_cr='D' AND log.cod_payment_txn LIKE '%SWIFT%' THEN 'TRF OUT SWIFT'
                             WHEN trx.cod_txn_mnemonic IN (1421,1424,1428,1429,1431,1432,1433,1434,1435,2102,7072) THEN 'INSTALLMENT'
                             WHEN REGEXP_CONTAINS (upper(trx.trx_description),"LOAN LIQUIDATION") = TRUE AND trx.cod_txn_mnemonic IN (1008) AND trx.dr_or_cr='D' THEN 'INSTALLMENT'
                        END AS trx_type
                      , CAST(trx.branch_code AS INT) AS branch_code_trx
                      , branch.branch_name AS branch_name_trx
                      , FORMAT_DATE('%Y%m%d', DATE(trx.trx_timestamp)) date_pr
                FROM trx
                LEFT JOIN ccy ON trx.date_pr2 = ccy.date_pr AND trx.ccycode_origin= ccy.cod_ccy
                LEFT JOIN branch ON trx.date_pr2 = branch.date_pr AND trx.branch_code= branch.branch_code_original
                LEFT JOIN mnemonic ON trx.date_pr2 = mnemonic.date_pr AND trx.cod_txn_mnemonic = mnemonic.cod_txn_mnemonic
                LEFT JOIN acc ON trx.date_pr2 = acc.date_pr AND trx.account_number = acc.account_number
                LEFT JOIN LOG ON trx.date_pr2 = log.date_pr AND trx.ref_no = log.ref_no
        )
    , bonds AS (SELECT 'BRANCH' AS channel_type
    					, acc.cif
    					, TRIM(acct_no) AS account_number
                        , acc.prod_code
                        , acc.prod_name
                        , acc.lob_code
                        , acc.lob_name
                        , trade_date AS trx_timestamp
                        , DATE(trade_date) AS trx_date
                        , CASE WHEN `position` = 'Nasabah Beli' THEN 'BUY BONDS' ELSE 'SELL BONDS' END AS trx_type
                        , CASE WHEN `position` = 'Nasabah Beli' THEN 'D' ELSE 'C' END AS dr_or_cr
                        , 'Approved' AS `result`
                        , 1 AS no_of_trx
                        , CASE WHEN currencies <> 'IDR' THEN net_proceed*rate ELSE net_proceed END AS amount_idr
                        , net_proceed AS amount_origin
                        , currencies AS ccynam_origin 
                        , CONCAT(IFNULL(series, ''), '-', IFNULL(`indicator`, '')) AS trx_description
                        , CAST(branch_code_rbs AS INT) AS branch_code_trx
                        , branch.branch_name AS branch_name_trx
                        , t_bonds.date_pr AS date_pr
                FROM `prj-7810ed85d543e33a`.dm_dap.t_bonds
                LEFT JOIN acc ON substr(t_bonds.date_pr,1,6) = substr(acc.date_pr,1,6) AND TRIM(t_bonds.acct_no) = acc.account_number
                LEFT JOIN branch ON CAST(t_bonds.branch_code_rbs AS string)= CAST(branch.branch_code_original AS string)
                WHERE 1=1
                --AND t_bonds.date_pr >='20240901' and t_bonds.date_pr <='20240930' 
                AND cek <> 'IPO'
        )              
    , mf AS (SELECT 'BRANCH' AS channel_type
                    , acc.cif AS cif
                    , lpad(stl_account_no, 12, '0') AS account_number
                    , acc.prod_code
                    , acc.prod_name
                    , acc.lob_code
                    , acc.lob_name
                    , trx_date AS trx_timestamp
                    , DATE(trx_date) AS trx_date
                    , concat('MF',' ',IFNULL(upper(TRIM(trx_type)),'')) AS trx_type
                    , CASE WHEN upper(TRIM(trx_type)) IN ('SUBSCRIPTION', 'SWITCHING IN') THEN 'C' ELSE 'D' END AS dr_or_cr
                    , 'Approved' AS `result`
                    , 1 AS no_of_trx
                    , ccy AS ccynam_origin
                    , net_amount+coalesce(fee_amount,0) AS amount_origin
                    , net_amount_idr+coalesce(fee_amount_idr,0) AS amount_idr
                    , CONCAT(IFNULL(product_group,''),'-',IFNULL(product_group2,''),'-',IFNULL(product_name,'')) AS trx_description
                    , CAST(br_code_avantrade AS INT) AS branch_code_trx
                    , branch.branch_name AS branch_name_trx
                    , tradedate2 AS date_pr
                FROM `prj-7810ed85d543e33a`.dm_dap.investment_mt_fund
                LEFT JOIN acc ON investment_mt_fund.date_pr = acc.date_pr AND lpad(investment_mt_fund.stl_account_no, 12, '0') = acc.account_number
                LEFT JOIN branch ON CAST(investment_mt_fund.br_code_avantrade AS string)= CAST(branch.branch_code_original AS string)
                WHERE 1=1 
                --AND investment_mt_fund.date_pr = '20240930' 
                AND substring(investment_mt_fund.date_pr,1,6) = tradedate4
                AND upper(TRIM(transactionstatus)) = 'ALLOCATED' 
                AND br_code_avantrade NOT IN ('99990')
                AND upper(TRIM(trx_type)) IN ('SWITCHING OUT', 'SUBSCRIPTION', 'REDEMPTION')
        )
    , fx AS (SELECT 'BRANCH' AS channel_type
                    , acc.cif
                    , acc.account_number
                    , acc.prod_code
                    , acc.prod_name
                    , acc.lob_code
                    , acc.lob_name
                    , 'FX TRANSACTION' AS trx_type
                    , 'D' AS dr_or_cr, 'Approved' AS `result`
                    , 1 AS no_of_trx
                    , dat_txn AS trx_timestamp
                    , DATE(dat_txn) AS trx_date
                    , CASE WHEN pors_ncbs = 'P' THEN amt_txn_to*rat_to_ccy ELSE amt_txn_from*rat_from_ccy END AS amount_idr
                    , amt_txn_from AS amount_origin
                    , nam_ccy_from AS ccynam_origin
                    , txt_txn_desc AS trx_description
                    , CAST(cod_cc_brn_txn AS INT) AS branch_code_trx
                    , branch.branch_name AS branch_name_trx
                    , new_investment_fx.date_pr AS date_pr
            FROM `prj-7810ed85d543e33a`.dm_dap.new_investment_fx
            LEFT JOIN acc ON new_investment_fx.date_pr = acc.date_pr AND new_investment_fx.account_number = acc.account_number 
            LEFT JOIN branch ON CAST(new_investment_fx.cod_cc_brn_txn AS string)= CAST(branch.branch_code_original AS string)
            WHERE 1=1
            -- AND new_investment_fx.date_pr >= '20240901' and new_investment_fx.date_pr <= '20240930' 
            AND new_investment_fx.cod_cc_brn_txn NOT IN ('9999') AND nam_ccy_from NOT IN ('IDR') AND source NOT IN ('V3')
            AND new_investment_fx.cod_cc_brn_txn IN (SELECT branch_code_original FROM branch)
        )        
    , mld AS (SELECT 'BRANCH' AS channel_type
                     , acc.cif AS cif
                     , lpad(SPLIT((CAST(t_mld.source_of_fund AS string)),'.')[SAFE_OFFSET(1)],12,'0') AS account_number
                     , acc.prod_code
                     , acc.prod_name
                     , acc.lob_code
                     , acc.lob_name
                     , 'SUBSCRIPTION MLD' AS trx_type
                     , 'D' AS dr_or_cr
                     , 'Approved' AS `result`
                     , 1 AS no_of_trx
                     , trade_date AS trx_timestamp
                     , DATE(trade_date) AS trx_date
                     , vol_in_idr AS amount_idr
                     , placement_amount AS amount_origin
                     , ccy AS ccynam_origin
                     , 'SUBSCRIPTION MLD' AS trx_description
                     , CAST(branch_code AS INT) AS branch_code_trx
                     , branch.branch_name AS branch_name_trx
                     , FORMAT_DATE('%Y%m%d', DATE(trade_date)) AS date_pr
                FROM `prj-7810ed85d543e33a`.dm_dap.t_mld
                LEFT JOIN acc ON t_mld.date_pr = acc.date_pr 
                			 AND lpad(SPLIT((CAST(t_mld.source_of_fund AS string)),'.')[SAFE_OFFSET(0)],12,'0') = acc.account_number 
                LEFT JOIN branch ON CAST(t_mld.branch_code AS string)= CAST(branch.branch_code_original AS string)
                WHERE 1=1
                -- AND t_mld.date_pr = '20240930'
        )
SELECT channel_type
       , cif
       , account_number
       , prod_code
       , prod_name
       , lob_code
       , lob_name
       , trx_timestamp
       , trx_date
       , trx_type
       , dr_or_cr
       , `result`
       , no_of_trx
       , amount_idr
       , amount_origin
       , ccynam_origin
       , trx_description
       , branch_code_trx
       , branch_name_trx
      -- , date_pr
FROM 
(
SELECT channel_type
       , cif
       , account_number
       , prod_code
       , prod_name
       , lob_code
       , lob_name
       , trx_timestamp
       , trx_date
       , trx_type
       , dr_or_cr
       , `result`
       , no_of_trx
       , amount_idr
       , amount_origin
       , ccynam_origin
       , trx_description
       , branch_code_trx
       , branch_name_trx
       , date_pr
FROM stg
UNION ALL
SELECT channel_type
       , cif
       , account_number
       , prod_code
       , prod_name
       , lob_code
       , lob_name
       , trx_timestamp
       , trx_date
       , trx_type
       , dr_or_cr
       , `result`
       , no_of_trx
       , amount_idr
       , amount_origin
       , ccynam_origin
       , trx_description
       , branch_code_trx
       , branch_name_trx
       , date_pr
FROM bonds
UNION ALL
SELECT channel_type
       , cif
       , account_number
       , prod_code
       , prod_name
       , lob_code
       , lob_name
       , TIMESTAMP(trx_timestamp) 
       , trx_date
       , trx_type
       , dr_or_cr
       , `result`
       , no_of_trx
       , amount_idr
       , amount_origin
       , ccynam_origin
       , trx_description
       , branch_code_trx
       , branch_name_trx
       , date_pr
FROM mf
UNION ALL
SELECT channel_type
       , cif
       , account_number
       , prod_code
       , prod_name
       , lob_code
       , lob_name
       , TIMESTAMP(trx_timestamp) 
       , trx_date
       , trx_type
       , dr_or_cr
       , `result`
       , no_of_trx
       , amount_idr
       , amount_origin
       , ccynam_origin
       , trx_description
       , branch_code_trx
       , branch_name_trx
       , date_pr
FROM fx
UNION ALL
SELECT channel_type
       , cif
       , account_number
       , prod_code
       , prod_name
       , lob_code
       , lob_name
       , trx_timestamp
       , trx_date
       , trx_type
       , dr_or_cr
       , `result`
       , no_of_trx
       , amount_idr
       , amount_origin
       , ccynam_origin
       , trx_description
       , branch_code_trx
       , branch_name_trx
       , date_pr
FROM mld
) AA 
ORDER BY trx_timestamp ASC