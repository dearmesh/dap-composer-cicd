WITH BD_MIS_CH_ACCT_MAST AS (
SELECT cod_cust 
      ,cod_acct_no 
      ,cod_prod 
      ,cod_ccy AS cod_ccy_acct
  FROM misplus.bd_mis_ch_acct_mast
 WHERE 1=1
--   AND date_pr = '20240930'
   AND cod_prod NOT IN (370, 371, 372, 379)
   AND cod_prod NOT BETWEEN 615 AND 626
  ),
BD_CH_NOBOOK AS (
SELECT *
  FROM misplus.bd_ch_nobook
 WHERE 1=1
--   AND date_pr = '20240930'
   AND cod_txn_mnemonic in (9990)
),
CH_NOBOOK_202409 AS (
SELECT 
       i.date_pr
      ,a.*
      ,i.*
      ,j.txt_txn_desc AS txt_txn_desc_mnemonic
      ,j.cod_base_txn_mnemonic
      ,j.txt_base_txn_desc
      ,j.cod_txn_category
      ,j.cod_txn_mode
      ,j.cod_org_channel
  FROM BD_CH_NOBOOK i
INNER JOIN BD_MIS_CH_ACCT_MAST a ON a.cod_acct_no = i.cod_acct_no
LEFT JOIN (SELECT *
             FROM misplus.bd_ba_txn_mnemonic
            WHERE 1=1
--              AND date_pr = '20240930'
              ) j ON i.cod_txn_mnemonic = j.cod_txn_mnemonic
 WHERE 1=1
--   AND j.cod_org_channel  = 'BRN'
--   AND j.cod_txn_category = 'CUST'
   AND i.cod_cc_brn_txn < 7000
   AND NOT ( UPPER(i.txt_txn_desc) LIKE '%ADM TRANSFER%' OR
             UPPER(i.txt_txn_desc) LIKE '%ADM TRNSFR%' OR 
             UPPER(i.txt_txn_desc) LIKE '%ADM TRF%' OR
             UPPER(i.txt_txn_desc) LIKE '%FEE WAIVE%' OR
             UPPER(i.txt_txn_desc) LIKE '%SERVICE CHARGE%' OR
             (UPPER(i.txt_txn_desc) LIKE '%FEE%' AND i.cod_txn_mnemonic IN (2622,2647)) OR 
             (i.ref_usr_no LIKE 'FEE%' AND i.cod_txn_mnemonic = 2622) OR
             (i.ref_usr_no LIKE '%REV%' AND i.cod_txn_mnemonic = 2622 AND AMT_TXN < 0 ) OR
             (i.txt_txn_desc = 'BAGI HASIL' AND i.cod_drcr = 'C' AND i.cod_txn_mnemonic = 2648 ) OR
             (i.cod_txn_mnemonic = 2647  AND i.cod_drcr = 'D' AND i.txt_txn_desc LIKE 'PAJAK%' ) OR
             (i.cod_txn_mnemonic = 1408  AND i.ref_usr_no = '3955' AND i.txt_txn_desc LIKE 'REV_PAJAK%' ) OR
             ((i.cod_userno = 3955 OR i.cod_cc_brn_txn = 9211) AND i.txt_txn_desc LIKE 'REV%' ) OR
             (i.ref_usr_no LIKE '8a42bdb%' AND i.txt_txn_desc LIKE '%PAJAK%' )
           )
),
CH_NOBOOK_202409_TRX_TYPE AS (
SELECT *
      ,CASE WHEN COD_TXN_MNEMONIC IN (1025, 1075, 1473, 1474, 1475, 1476, 1477, 2282, 2329, 2330) THEN 'PAYMENT_PURCHASE'
            WHEN COD_TXN_MNEMONIC IN (1401, 1453, 1514, 2250, 2236) AND COD_DRCR = 'C' THEN 'CASH_DEPOSIT'
            WHEN COD_TXN_MNEMONIC IN (6601, 6501) AND COD_DRCR = 'C' THEN 'CHEQUE_DEPOSIT'
--            WHEN COD_TXN_MNEMONIC = 1015 AND REGEXP_CONTAINS(UPPER(TXT_TXN_DESC), r"Str Dep Debit CASA/TD\.P") > FALSE AND COD_DRCR = 'D' THEN 'YYY' --- belum lengkap
            WHEN COD_TXN_MNEMONIC = 1015 AND REGEXP_CONTAINS(UPPER(TXT_TXN_DESC), r"/CMM/") = FALSE AND COD_DRCR = 'D' THEN 'OPEN_TD'
--            WHEN COD_TXN_MNEMONIC IN (1006,1015,1320,1702,1710,2204,2205,2269,2289,2305,2647,2654,2640,2670,7606,7607,2212,1703,) --- belum lengkap
            WHEN REGEXP_CONTAINS(UPPER(TXT_TXN_DESC), r"^Internet Trf") > FALSE THEN 'OVERBOOKING'
            WHEN COD_TXN_MNEMONIC IN (2271, 2273, 2295, 2297, 2311, 2315) AND COD_DRCR = 'D' THEN 'ONLINE_TRF_OUT'
            WHEN COD_TXN_MNEMONIC IN (2382) AND COD_DRCR = 'D' THEN 'BIFAST_TRF_OUT'
--            WHEN COD_TXN_MNEMONIC IN (2622) AND UPPER(SUBSTRING(REGEXP_REPLACE(TXT_TXN_DESC, r"\s", ""), 1, 6)) = 'BIFAST' AND COD_DRCR = 'D' THEN 'YYY' --- belum lengkap
            WHEN COD_TXN_MNEMONIC IN (1001, 1013, 2201, 2267, 2283, 2285, 2301, 2353) AND COD_DRCR = 'D' THEN 'CASH_WITHDRAWAL'
            WHEN COD_TXN_MNEMONIC IN (6101) AND COD_DRCR = 'D' THEN 'CHEQUE_WITHDRAWAL'
            WHEN COD_TXN_MNEMONIC IN (1008) AND COD_DRCR = 'D' AND (SUBSTRING(REF_USR_NO,1,4) IN ('SKN0','RTGS') OR REF_USR_NO = '2030') THEN 'YYY' --- belum lengkap
            WHEN COD_TXN_MNEMONIC IN (1421, 1424, 1428, 1429, 1431, 1432, 1433, 1434, 1435, 2102, 7072, 9990) THEN 'INSTALLMENT'
            WHEN REGEXP_CONTAINS(UPPER(TXT_TXN_DESC), r"LOAN LIQUIDATION") > FALSE AND COD_TXN_MNEMONIC IN (1008) AND COD_DRCR = 'D' THEN 'INSTALLMENT' --- belum lengkap
            ELSE 'OTHERS'
        END AS TRX_TYPE
       ,1 AS TRX
       ,CASE WHEN RAT_CONV_TCLCY = 0 THEN AMT_TXN
             ELSE AMT_TXN_TCY * RAT_CONV_TCLCY
         END AS TAX_AMOUNT_IDR
  FROM CH_NOBOOK_202409
),
CH_NOBOOK_202409_TRX_TYPE_SUMMARY AS (
SELECT TRX_TYPE
      ,COD_TXN_MNEMONIC
      ,COUNT(1)
  FROM CH_NOBOOK_202409_TRX_TYPE
 WHERE 1=1
GROUP BY 1,2
)
SELECT *
      ,CASE WHEN TRX_TYPE IN ('MERCHANT_SETL') THEN 'Merchant Setl'
            WHEN TRX_TYPE IN ('CASH_DEPOSIT') THEN 'Cash Deposit'
            WHEN TRX_TYPE IN ('BIFAST_TRF_IN','LLG_RTGS_TRF_IN','ONLINE_TRF_IN','TRF_IN_PYMT_GTW') THEN 'Interbank Transfer In' --- belum lengkap
            WHEN TRX_TYPE IN ('CASH_WDW') THEN 'Cash Withdraw'
            WHEN TRX_TYPE IN ('BIFAST_TRF_OUT','LLG_RTGS_TRF_OUT','ONLINE_TRF_OUT') THEN 'Interbank Transfer Out'
            WHEN TRX_TYPE IN ('BONDS BUY','MF BUY','BANCA') THEN 'Move to WM'
            WHEN TRX_TYPE IN ('BONDS SALE','MF SALE','BONDS INTEREST') THEN 'Move from WM'
            WHEN TRX_TYPE IN ('OPEN_TD') THEN 'Move to TD'
            WHEN TRX_TYPE IN ('TD_MATURE','TD_INTEREST') THEN 'Move from TD'
            WHEN TRX_TYPE IN ('INSTALLMENT','DISBURSEMENT ADIRA') THEN 'Installment'
            WHEN TRX_TYPE IN ('PAYROLL','OVERBOOKING') THEN 'Overbooking'
            WHEN TRX_TYPE IN ('PAYMENT_PURCHASE','POS_TRX','TOPUP_EWALLET') THEN 'Payment Purchase'
            WHEN TRX_TYPE IN ('OTHERS','CLOSED ACCOUNT') THEN 'Others'
            ELSE TRX_TYPE
        END AS TRX_TYP2
  FROM CH_NOBOOK_202409_TRX_TYPE




  ----- query dari pak nico -----

  alter view dmu_workspace.vw_nrp_trx_si as
with 
    acc as
    (
    select cif, trim(COD_ACCT_NO) as account_number, 
    prod_code, 
    prod_name, 
    lob_code,
    CASE WHEN lob_code IN(10) THEN 'CMM'
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
         WHEN lob_code = 0 THEN 'BLANK' ELSE 'BLANK' END as lob_name,
    date_pr
    from bd_tableau.dm_casa_funding
    where date_pr='20240930' 
	and prod_code not in (370,371,372,379)
    and not (prod_code between 615 and 626)
    ),
    ccy as
    (
    select cod_ccy, nam_ccy_short, date_pr
    from newmisplus2.bd_ba_ccy_code
    where date_pr = '20240930'
    ),
    mnemonic as
    (
    select 
    TXT_TXN_DESC as TXT_TXN_DESC_MNEMONIC,
    COD_TXN_MNEMONIC,
    TXT_BASE_TXN_DESC,
    COD_TXN_CATEGORY,
    COD_TXN_MODE,
    COD_ORG_CHANNEL,
    date_pr
    from newmisplus2.BD_BA_TXN_MNEMONIC
    where date_pr='20240930' 
    and cod_txn_mnemonic in (1428,1434,2102,9990)
    ),
    trx as
    (
    select 'SI' as channel_type, dat_txn as trx_timestamp, to_date(dat_txn) as trx_date, 
    cod_userno,
    trim(ref_txn_no) as ref_no, 
    trim(ref_usr_no) as ref_usr_no, 
    trim(cod_acct_no) as account_number, 
    cod_auth_id,
    cod_txn_mnemonic,
    cod_drcr as dr_or_cr,
    'Approved' as `result`, 
    1 as no_of_trx, 
    amt_txn_tcy * rat_conv_tclcy as amount_idr, 
    amt_txn_tcy as amount_origin,
    cod_txn_ccy as ccycode_origin, 
    rat_conv_tclcy as ccy_rate,
    txt_txn_desc as trx_description,
    cast(cod_cc_brn_txn as string) as branch_code,
    date_pr
    from newmisplus2.BD_CH_NOBOOK
    where date_pr='20240930'
    and cod_txn_mnemonic in (select cod_txn_mnemonic from mnemonic)
    ),
    stg as
    (
    select trx.*,ccy.nam_ccy_short as ccynam_origin, acc.cif, acc.prod_code, acc.prod_name, acc.lob_code, acc.lob_name,
    mnemonic.txt_txn_desc_mnemonic,mnemonic.txt_base_txn_desc,mnemonic.cod_txn_category,mnemonic.cod_txn_mode,mnemonic.cod_org_channel,
    case when trx.COD_TXN_MNEMONIC IN (1075,1321,1452,1466,1467,1471,1481,1482,2282,2329,2330) then 'PAYMENT PURCHASE'
    when trx.COD_TXN_MNEMONIC in (1401,2250,2236) and trx.dr_or_cr='C' then 'CASH DEPOSIT'
    when trx.COD_TXN_MNEMONIC in (1453,1514,6601,6501) and trx.dr_or_cr='C' then 'CHEQUE DEPOSIT'
    when trx.COD_TXN_MNEMONIC in (1001,1301,2201,2267,2283,2285,2301,2353) and trx.dr_or_cr='D' then 'CASH WITHDRAWAL'
    when trx.COD_TXN_MNEMONIC in (1013,6101) and trx.dr_or_cr='D' then 'CHEQUE WITHDRAWAL'
    when trx.COD_TXN_MNEMONIC in (1006,1015,1320,1321,1702,1710,2204,2205,2269,2289,2305,2647,2654,2640,2670,7606,7607,2212,1703,1704) then 'TRF OVERBOOKING'
    when trx.COD_TXN_MNEMONIC in (2271,2273,2295,2297,2311,2315) and trx.dr_or_cr='D' then 'TRF OUT ONLINE'
    when trx.COD_TXN_MNEMONIC in (2382) and trx.dr_or_cr='D' then 'TRF OUT BI FAST'
    when trx.COD_TXN_MNEMONIC in (2622) and substr(upper(trim(trx.trx_description)),1,6)='BIFAST' and trx.dr_or_cr='D' then 'TRF OUT BI FAST'
    when trx.COD_TXN_MNEMONIC in (1421,1424,1428,1429,1431,1432,1433,1434,1435,2102,7072) then 'INSTALLMENT'
    when regexp_like(upper(trx.trx_description),"LOAN LIQUIDATION")>0 and trx.COD_TXN_MNEMONIC in (1008) and trx.dr_or_cr='D' then 'INSTALLMENT'
    when trx.COD_TXN_MNEMONIC in (9990) and regexp_like(upper(trx.trx_description),'TABUNGAN|RENCANA|HAJI|PREMI|ASURANSI')=1 then 'INSTALLMENT'
    when trx.COD_TXN_MNEMONIC in (9990) and regexp_like(upper(trx.trx_description),'TRANSFER')=1 then 'TRF OVERBOOKING'   
    when trx.COD_TXN_MNEMONIC in (9990) and regexp_like(upper(trx.trx_description),'TABUNGAN|RENCANA|HAJI|PREMI|ASURANSI|TRANSFER')=0 then 'PAYMENT PURCHASE'    
    end as trx_type
    from trx
    left join ccy on trx.date_pr = ccy.date_pr and trx.ccycode_origin= ccy.cod_ccy
    left join mnemonic on trx.date_pr = mnemonic.date_pr and trx.cod_txn_mnemonic = mnemonic.cod_txn_mnemonic
    left join acc on trx.date_pr = acc.date_pr and trx.account_number = acc.account_number
),
    drip as
    (
    select 'SI' as channel_type, cast(cust_cif as bigint) as cif, lpad(stl_account_no, 12, '0') as account_number,
    acc.prod_code, acc.prod_name, acc.lob_code, acc.lob_name,
    trx_date as trx_timestamp, tradedate2 as trx_date, 
    'SUBSCRIPTION MF RSP' as trx_type,
    'D' as dr_or_cr,
    'Approved' as `result`, 1 as no_of_trx, ccy as ccynam_origin,
    net_amount+coalesce(fee_amount,0) as amount_origin,
    net_amount_idr+coalesce(fee_amount_idr,0) as amount_idr,
    concat_ws('-',product_group,product_group2,product_name) as trx_description,
    tradedate3
    from bd_tableau.investment_mt_fund
    left join acc on bd_tableau.investment_mt_fund.date_pr = acc.date_pr and lpad(bd_tableau.investment_mt_fund.stl_account_no, 12, '0') = acc.account_number
    where bd_tableau.investment_mt_fund.date_pr = '20240930' and substring(bd_tableau.investment_mt_fund.date_pr,1,6) = tradedate4
    and upper(trim(transactionstatus)) = 'ALLOCATED' 
    AND upper(trim(trx_type)) IN ('RSP')
    )

select channel_type, cif, account_number, prod_code, prod_name, lob_code, lob_name,  
trx_timestamp, trx_date, trx_type, dr_or_cr, `result`, no_of_trx, 
amount_idr, amount_origin, ccynam_origin, trx_description,
from_timestamp(trx_timestamp, 'yyyyMMdd') as date_pr
from stg
union all
select channel_type, cif, account_number, prod_code, prod_name, lob_code, lob_name,  
trx_timestamp, trx_date, trx_type, dr_or_cr, `result`, no_of_trx, 
amount_idr, amount_origin, ccynam_origin, trx_description,
from_timestamp(trx_timestamp, 'yyyyMMdd') as date_pr
from drip
order by trx_timestamp asc



---- versi bq -----
  WITH acc AS (
               SELECT cif
                     ,trim(COD_ACCT_NO) AS account_number
                     ,prod_code
                     ,prod_name
                     ,lob_code
                     ,CASE WHEN lob_code IN(10) THEN 'CMM'
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
                           WHEN lob_code = 0 THEN 'BLANK' ELSE 'BLANK'
                       END AS lob_name
                     ,date_pr
                 FROM dm_dap.dm_casa_funding
                WHERE 1=1
               --    AND date_pr='20241201' 
               	AND prod_code NOT IN (370,371,372,379)
                AND NOT (prod_code BETWEEN 615 AND 626)
  )
  ,ccy AS (
           SELECT cod_ccy
		         ,nam_ccy_short
				 ,date_pr
             FROM misplus.bd_ba_ccy_code
            WHERE 1=1
  --            AND date_pr = '20240930'
  )  
  ,mnemonic AS (
                SELECT 
                      TXT_TXN_DESC as TXT_TXN_DESC_MNEMONIC
                     ,COD_TXN_MNEMONIC
                     ,TXT_BASE_TXN_DESC
                     ,COD_TXN_CATEGORY
                     ,COD_TXN_MODE
                     ,COD_ORG_CHANNEL
                     ,date_pr
                  FROM misplus.bd_ba_txn_mnemonic
                 WHERE 1=1
            --      AND date_pr='20240930' 
                   AND cod_txn_mnemonic in (1428,1434,2102,9990)
  ) 
  ,trx AS (
           SELECT 'SI' AS channel_type
                 ,dat_txn AS trx_timestamp
                 ,DATE(dat_txn) AS trx_date
                 ,cod_userno
                 ,TRIM(ref_txn_no) AS ref_no
                 ,TRIM(ref_usr_no) AS ref_usr_no
                 ,TRIM(cod_acct_no) AS account_number
                 ,cod_auth_id
                 ,cod_txn_mnemonic
                 ,cod_drcr AS dr_or_cr
                 ,'Approved' AS `result`
                 ,1 AS no_of_trx
                 ,amt_txn_tcy * rat_conv_tclcy AS amount_idr
                 ,amt_txn_tcy AS amount_origin
                 ,cod_txn_ccy AS ccycode_origin
                 ,rat_conv_tclcy AS ccy_rate
                 ,txt_txn_desc AS trx_description
                 ,CAST(cod_cc_brn_txn AS STRING) AS branch_code
                 ,date_pr
             FROM misplus.bd_ch_nobook
            WHERE 1=1
  --		    AND date_pr='20240930'
              AND cod_txn_mnemonic IN (SELECT cod_txn_mnemonic
                                         FROM mnemonic)
  )
  ,stg AS (
           SELECT trx.*
                 ,ccy.nam_ccy_short AS ccynam_origin
                 ,acc.cif
                 ,acc.prod_code
                 ,acc.prod_name
                 ,acc.lob_code
                 ,acc.lob_name
                 ,mnemonic.txt_txn_desc_mnemonic
                 ,mnemonic.txt_base_txn_desc
                 ,mnemonic.cod_txn_category
                 ,mnemonic.cod_txn_mode
                 ,mnemonic.cod_org_channel
                 ,CASE WHEN trx.COD_TXN_MNEMONIC IN (1075,1321,1452,1466,1467,1471,1481,1482,2282,2329,2330) THEN 'PAYMENT PURCHASE'
                       WHEN trx.COD_TXN_MNEMONIC IN (1401,2250,2236) AND trx.dr_or_cr = 'C' THEN 'CASH DEPOSIT'
                       WHEN trx.COD_TXN_MNEMONIC IN (1453,1514,6601,6501) AND trx.dr_or_cr = 'C' THEN 'CHEQUE DEPOSIT'
                       WHEN trx.COD_TXN_MNEMONIC IN (1001,1301,2201,2267,2283,2285,2301,2353) AND trx.dr_or_cr = 'D' THEN 'CASH WITHDRAWAL'
                       WHEN trx.COD_TXN_MNEMONIC IN (1013,6101) AND trx.dr_or_cr = 'D' THEN 'CHEQUE WITHDRAWAL'
                       WHEN trx.COD_TXN_MNEMONIC IN (1006,1015,1320,1321,1702,1710,2204,2205,2269,2289,2305,2647,2654,2640,2670,7606,7607,2212,1703,1704) THEN 'TRF OVERBOOKING'
                       WHEN trx.COD_TXN_MNEMONIC IN (2271,2273,2295,2297,2311,2315) AND trx.dr_or_cr = 'D' THEN 'TRF OUT ONLINE'
                       WHEN trx.COD_TXN_MNEMONIC IN (2382) AND trx.dr_or_cr = 'D' THEN 'TRF OUT BI FAST'
                       WHEN trx.COD_TXN_MNEMONIC IN (2622) AND substr(upper(trim(trx.trx_description)),1,6) = 'BIFAST' AND trx.dr_or_cr = 'D' THEN 'TRF OUT BI FAST'
                       WHEN trx.COD_TXN_MNEMONIC IN (1421,1424,1428,1429,1431,1432,1433,1434,1435,2102,7072) THEN 'INSTALLMENT'
                       WHEN REGEXP_CONTAINS(UPPER(trx.trx_description),"LOAN LIQUIDATION") > FALSE AND trx.COD_TXN_MNEMONIC IN (1008) AND trx.dr_or_cr = 'D' THEN 'INSTALLMENT'
                       WHEN trx.COD_TXN_MNEMONIC IN (9990) AND REGEXP_CONTAINS(upper(trx.trx_description),'TABUNGAN|RENCANA|HAJI|PREMI|ASURANSI') = TRUE THEN 'INSTALLMENT'
                       WHEN trx.COD_TXN_MNEMONIC IN (9990) AND REGEXP_CONTAINS(upper(trx.trx_description),'TRANSFER') = TRUE THEN 'TRF OVERBOOKING'   
                       WHEN trx.COD_TXN_MNEMONIC IN (9990) AND REGEXP_CONTAINS(upper(trx.trx_description),'TABUNGAN|RENCANA|HAJI|PREMI|ASURANSI|TRANSFER') = FALSE THEN 'PAYMENT PURCHASE'    
                   END AS trx_type
            FROM trx
       LEFT JOIN ccy ON trx.date_pr = ccy.date_pr
                    AND trx.ccycode_origin = ccy.cod_ccy
       LEFT JOIN mnemonic ON trx.date_pr = mnemonic.date_pr
                         AND trx.cod_txn_mnemonic = mnemonic.cod_txn_mnemonic
       LEFT JOIN acc ON trx.date_pr = acc.date_pr
                    AND trx.account_number = acc.account_number
  )
  ,drip AS (
            SELECT 'SI' AS channel_type
                  ,CAST(imf.cust_cif AS BIGINT) AS cif
                  ,LPAD(imf.stl_account_no, 12, '0') AS account_number
                  ,acc.prod_code
                  ,acc.prod_name
                  ,acc.lob_code
                  ,acc.lob_name
                  ,imf.trx_date AS trx_timestamp
                  ,imf.tradedate2 AS trx_date
                  ,'SUBSCRIPTION MF RSP' AS trx_type
                  ,'D' AS dr_or_cr
                  ,'Approved' AS `result`
                  ,1 AS no_of_trx
                  ,imf.ccy AS ccynam_origin
                  ,imf.net_amount+COALESCE(imf.fee_amount,0) AS amount_origin
                  ,imf.net_amount_idr+COALESCE(imf.fee_amount_idr,0) AS amount_idr
                  ,ARRAY_TO_STRING(ARRAY[imf.product_group,imf.product_group2,imf.product_name],'-') AS trx_description
                  ,imf.tradedate3
              FROM dm_dap.investment_mt_fund imf
            LEFT JOIN acc ON 1=1
                         AND imf.date_pr = acc.date_pr
                         AND LPAD(imf.stl_account_no, 12, '0') = acc.account_number
             WHERE 1=1
        --       AND imf.date_pr = '20240930'
               AND SUBSTRING(imf.date_pr,1,6) = tradedate4
               AND UPPER(TRIM(transactionstatus)) = 'ALLOCATED' 
               AND UPPER(TRIM(trx_type)) IN ('RSP')
  )
  ,vw_nrp_trx_si AS (
  SELECT channel_type
        ,cif
        ,account_number
        ,prod_code
        ,prod_name
        ,lob_code
        ,lob_name
        ,TIMESTAMP(trx_timestamp) AS trx_timestamp
        ,DATE(trx_date) AS trx_date
        ,trx_type
        ,dr_or_cr
        ,`result`
        ,no_of_trx
        ,amount_idr
        ,amount_origin
        ,ccynam_origin
        ,trx_description
        --  FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP(trx_timestamp)) AS date_pr --- Perlu Konfirmasi terkait kolom date_pr, apakah selalu month -1 dari cursor, atau date_pr selalu di hasilkan dari data.
    FROM stg
  UNION ALL
  SELECT channel_type
        ,cif
        ,account_number
        ,prod_code
        ,prod_name
        ,lob_code 
        ,lob_name
        ,TIMESTAMP(trx_timestamp) AS trx_timestamp
        ,DATE(trx_date) AS trx_date
        ,trx_type
        ,dr_or_cr
        ,`result`
        ,no_of_trx
        ,amount_idr
        ,amount_origin
        ,ccynam_origin
        ,trx_description
        --  FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP(trx_timestamp)) AS date_pr --- Perlu Konfirmasi terkait kolom date_pr, apakah selalu month -1 dari cursor, atau date_pr selalu di hasilkan dari data.
    FROM drip
  )
  SELECT *
    FROM vw_nrp_trx_si
   WHERE 1=1
  ORDER BY TIMESTAMP(trx_timestamp) ASC