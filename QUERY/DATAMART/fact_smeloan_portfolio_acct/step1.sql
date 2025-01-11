--CREATE TABLE test.funding_a AS
--CREATE TEMPORARY TABLE funding_a AS
SELECT
    a.LOB_CODE
    , CAST(a.CIF_KEY AS STRING) AS CIF
    , a.ACCOUNT_NUMBER
    , a.ISO_CURRENCY_CD
    , DATE(a.ACCOUNT_OPEN_DATE) AS ACC_OPEN_DATE
    , DATE(a.ACCOUNT_CLOSE_DATE) AS ACC_CLOSE_DATE
    , TRIM(CAST(a.ACCOUNT_STATUS AS STRING)) AS FM_ORI_ACC_STATUS
    , a.BALANCE_IDR
    , a.CREDIT_LINE_ID
    , a.PRODUCT_DESC
    , REPLACE(TRIM(CAST(a.PRODUCT_CODE_NCBS AS STRING)), ".0", "") AS PRODUCT_CODE_NCBS
    , a.LINE_STATUS
    , d.description AS line_stat_desc
    , a.LIMIT_PLAFON_IDR
    , 0 AS COLLECTIBILITY
    , COALESCE(b.type, a.PRODUCT_TYP) AS PROD_GRP_TYPE
    , a.SOURCE_DATA AS DATA_SOURCE_A
    , "ffm_casa" AS DATA_SOURCE_B
    , ROUND(CAST(a.CUR_NET_RATE AS NUMERIC),3) AS RATE
    , c.OFSA_DESCRIPTION AS ACCOUNT_STATUS
    , a.AMT_HLD AS AMOUNT_HOLD
    , DATE(a.LINE_START_DATE) AS FM_LINE_START_DT
    , DATE(a.MATURITY_DATE) AS FM_MATURITY_DATE
    , a.AO_BUSINESS
FROM MISPLUS.FM_ACCT_CASA a
LEFT JOIN DM_DAP.DM_LEN_BCS_SME_PROD_MAPV2 b 
ON 1=1
	AND REPLACE(TRIM(CAST(a.PRODUCT_CODE_NCBS AS STRING)), ".0", "") = TRIM(b.PRODUCT_CODE_NCBS)
LEFT JOIN DM_DAP.DM_LEN_SME_ACC_STAT c 
ON 1=1
	AND TRIM(CAST(a.ACCOUNT_STATUS AS STRING)) = TRIM(CAST(c.MIS_ACCOUNT_STATUS AS STRING))
LEFT JOIN DM_DAP.DM_LEN_SME_LINE_STAT d 
ON 1=1
	AND TRIM(CAST(a.LINE_STATUS AS STRING)) = TRIM(d.CL_STATUS)
WHERE 1=1
--    AND a.DATE_PR = FORMAT_DATE('%Y%m%d', DATE :cursor_PROCESS_BUSINESS_DATE) -- '20231128' 
    AND a.LOB_CODE IN (22, 29, 31, 36, 30, 27, 35)

--========================================================================================================

--CREATE TEMPORARY TABLE funding_b AS
SELECT DISTINCT
    a.LOB_CODE
    , CAST(a.CIF_KEY AS STRING) AS CIF
    , a.ACCOUNT_NUMBER
    , a.ISO_CURRENCY_CD
    , DATE(a.ACCOUNT_OPEN_DATE) AS ACC_OPEN_DATE
    , DATE(a.ACCOUNT_CLOSE_DATE) AS ACC_CLOSE_DATE
    , TRIM(CAST(a.ACCOUNT_STATUS AS STRING)) AS FM_ORI_ACC_STATUS
    , a.BALANCE_IDR
    , '' AS CREDIT_LINE_ID
    , a.PRODUCT_DESC
    , REPLACE(TRIM(CAST(a.PRODUCT_CODE_NCBS AS STRING)), '.0', '') AS PRODUCT_CODE_NCBS
    , '' AS LINE_STATUS
    , '' AS LINE_STAT_DESC
    , 0 AS LIMIT_PLAFON_IDR
    , 0 AS COLLECTIBILITY
    , COALESCE(b.TYPE, a.PRODUCT_TYP) AS PROD_GRP_TYPE,
    , a.SOURCE_DATA AS DATA_SOURCE_A
    , 'ffm_td' AS DATA_SOURCE_B
    , a.CUR_NET_RATE AS RATE
    , c.OFSA_DESCRIPTION AS ACCOUNT_STATUS,
    , 0 AS AMOUNT_HOLD
    , CAST(NULL AS DATE) AS FM_LINE_START_DT
    , DATE(a.MATURITY_DATE) AS FM_MATURITY_DATE
    , a.AO_BUSINESS
FROM MISPLUS.FM_ACCT_TD a
LEFT JOIN DM_DAP.DM_LEN_BCS_SME_PROD_MAPV2 b 
ON 1=1
	AND REPLACE(TRIM(CAST(a.PRODUCT_CODE_NCBS AS STRING)), '.0', '') = TRIM(CAST(b.PRODUCT_CODE_NCBS AS STRING)) 
	AND LOWER(TRIM(b.class)) = 'funding'
LEFT JOIN DM_DAP.DM_LEN_SME_ACC_STAT c 
ON 1=1
	AND TRIM(CAST(a.account_status AS STRING)) = TRIM(CAST(c.MIS_Account_Status AS STRING))
WHERE 1=1
--	AND a.DATE_PR = FORMAT_DATE('%Y%m%d', DATE :cursor_PROCESS_BUSINESS_DATE) -- '20230831' 
    AND a.lob_code IN (36, 22, 30, 29, 27, 31, 35)
    
    
--========================================================================================================
    
--CREATE TEMPORARY TABLE lending_a AS
SELECT
    a.LOB_CODE
    , CAST(a.CIF_KEY AS STRING) AS CIF
    , a.ACCOUNT_NUMBER
    , a.ISO_CURRENCY_CD
    , DATE(a.ACCOUNT_OPEN_DATE) AS ACC_OPEN_DATE
    , DATE(a.ACCOUNT_CLOSE_DATE) AS ACC_CLOSE_DATE
    , a.ACCOUNT_STATUS AS fm_ori_acc_status
    , CASE 
        WHEN (a.BALANCE_IDR - a.AMT_ARREARS_DUE) IS NULL THEN a.BALANCE_IDR 
        ELSE (a.BALANCE_IDR - a.AMT_ARREARS_DUE) 
    END AS BALANCE_IDR
    , a.CREDIT_LINE_ID
    , a.PRODUCT_DESC
    , REPLACE(TRIM(CAST(a.PRODUCT_CODE_NCBS AS STRING)), '.0', '') AS PRODUCT_CODE_NCBS
    , a.LINE_STATUS
    , d.DESCRIPTION AS LINE_STAT_DESC,
    , a.LIMIT_PLAFON_IDR
    , CASE 
        WHEN CAST(a.COLLECTIBILITY AS STRING) LIKE '5%' THEN 5
        WHEN CAST(a.COLLECTIBILITY AS STRING) LIKE '4%' THEN 4
        WHEN a.COLLECTIBILITY = 10 THEN 1
        WHEN a.COLLECTIBILITY = 20 THEN 2
        WHEN a.COLLECTIBILITY = 30 THEN 3
        ELSE a.COLLECTIBILITY
    END AS COL
    , COALESCE(b.TYPE, a.PRODUCT_TYP) AS PROD_GRP_TYPE
    , a.SOURCE_DATA AS DATA_SOURCE_A
    , 'lfm_loan' AS DATA_SOURCE_B
    , a.CUR_NET_RATE AS RATE
    , c.OFSA_DESCRIPTION AS ACCOUNT_STATUS
    , 0 AS AMOUNT_HOLD
    , DATE(a.LINE_START_DATE) AS FM_LINE_START_DT
    , DATE(a.MATURITY_DATE) AS FM_MATURITY_DATE
    , a.AO_BUSINESS
FROM MISPLUS.FM_ACCT_LOAN a
LEFT JOIN DM_DAP.DM_LEN_BCS_SME_PROD_MAPV2 b 
ON 1=1
	AND REPLACE(TRIM(CAST(a.PRODUCT_CODE_NCBS AS STRING)), '.0', '') = TRIM(CAST(b.PRODUCT_CODE_NCBS AS STRING))
LEFT JOIN DM_DAP.DM_LEN_SME_ACC_STAT c 
ON 1=1
	AND TRIM(a.ACCOUNT_STATUS) = TRIM(CAST(c.MIS_ACCOUNT_STATUS AS STRING))
LEFT JOIN DM_DAP.DM_LEN_SME_LINE_STAT d 
ON 1=1
	AND TRIM(a.line_status) = TRIM(d.CL_Status)
WHERE 1=1
--	AND a.DATE_PR = FORMAT_DATE('%Y%m%d', DATE :cursor_PROCESS_BUSINESS_DATE) -- '20231128' 
    AND a.line_status IN ('A', 'O', '') 
    AND a.lob_code IN (22, 29, 31)
    
--===========================================================================================================

--CREATE TEMPORARY TABLE all_acc AS
SELECT * FROM funding_a
UNION ALL
SELECT * FROM funding_b
UNION ALL
SELECT * FROM lending_a

--===========================================================================================================

--CREATE TEMPORARY TABLE sme_portfolio AS
SELECT DISTINCT *
    , 'loan_active' AS acc_type_a
    , 'sme_acc' AS acc_type_b 
FROM 
    all_acc
WHERE 
    AND DATA_SOURCE_B = 'lfm_loan'
    AND LOB_CODE IN (22, 29, 31)
    AND LINE_STATUS IN ('A', 'O', '')
    AND NOT (PROD_GRP_TYPE IN ('TF') AND TRIM(CAST(FM_ORI_ACC_STATUS AS STRING)) = '0')
    AND NOT (PROD_GRP_TYPE IN ('KAB') AND (TRIM(CAST(FM_ORI_ACC_STATUS AS STRING)) = '1' OR BALANCE_IDR = 0))
UNION ALL
SELECT DISTINCT *
    , 'fund_loan_active' AS ACC_TYPE_A
    , 'sme_acc' AS ACC_TYPE_B 
FROM 
    all_acc
WHERE 1=1
    AND DATA_SOURCE_B = 'ffm_casa'
    AND LOB_CODE IN (22, 29, 31)
    AND LINE_STATUS IN ('O')
    AND PROD_GRP_TYPE NOT IN ('SA')
    AND TRIM(CAST(FM_ORI_ACC_STATUS AS STRING)) NOT IN ('1', '5')
UNION ALL
SELECT DISTINCT * 
    , 'fund_td' AS ACC_TYPE_A
    , 'sme_acc' AS ACC_TYPE_B 
FROM 
    all_acc
WHERE 1=1
    AND DATA_SOURCE_B = 'ffm_td'
    AND LOB_CODE IN (36, 22, 30, 29, 27, 31, 35)
    AND TRIM(CAST(FM_ORI_ACC_STATUS AS STRING)) NOT IN ('1', '5')
UNION ALL
SELECT DISTINCT * 
    , 'fund_casa' AS ACC_TYPE_A 
    , 'sme_acc' AS ACC_TYPE_B 
FROM 
    all_acc
WHERE 1=1
    AND ((
	        DATA_SOURCE_B = 'ffm_casa' 
	        AND LOB_CODE IN (22, 29, 31) 
	        AND LINE_STATUS NOT IN ('O') 
	        AND TRIM(CAST(FM_ORI_ACC_STATUS AS STRING)) NOT IN ('1', '5')
	    )
	    OR 
	    (
	        DATA_SOURCE_B = 'ffm_casa'
	        AND LOB_CODE IN (36, 30, 27, 35)
	        AND TRIM(CAST(FM_ORI_ACC_STATUS AS STRING)) NOT IN ('1', '5')
	    )
	    OR
	    (
	        DATA_SOURCE_B = 'ffm_casa' 
	        AND LOB_CODE IN (22, 29, 31) 
	        AND PROD_GRP_TYPE IN ('SA')
	        AND TRIM(CAST(FM_ORI_ACC_STATUS AS STRING)) NOT IN ('1', '5')
	    ))
	    
--===============================================================================
	    
--CREATE TEMPORARY TABLE ch_od_lim_a AS
--SELECT *
--FROM MISPLUS.BD_CH_OD_LIMIT
--WHERE 1=1
----	AND date_pr = 'YYYYMMDD'
--; 
-- PINDAH KE BAWAH LANGSUNG DIGABUNG
    
--==================================================================================

--CREATE TEMPORARY TABLE ch_od_lim AS
--SELECT 
--    ch_od_lim_a.*
--    , ROW_NUMBER() OVER (PARTITION BY cod_acct_no ORDER BY DAT_LIMIT_END DESC) AS REFF
--FROM 
--    (
--    	SELECT *
--		FROM MISPLUS.BD_CH_OD_LIMIT
--		WHERE 1=1
--			AND date_pr = '20241031'
--	) ch_od_lim_a
--ORDER BY 
--    cod_acct_no, DAT_LIMIT_END DESC;
	    
-- PINDAH KE BAWAH LANGSUNG DIGABUNG
	    
--==================================================================================

--CREATE TEMPORARY TABLE ch_od_lim_final AS
SELECT 
	ch_od_lim.*
FROM
(
	SELECT 
	    ch_od_lim_a.*
	    , ROW_NUMBER() OVER (PARTITION BY cod_acct_no ORDER BY DAT_LIMIT_END DESC) AS REFF
	FROM 
	    (
	    	SELECT *
			FROM MISPLUS.BD_CH_OD_LIMIT
			WHERE 1=1
				AND date_pr = FORMAT_DATE('%Y%m%d', DATE :cursor_PROCESS_BUSINESS_DATE) --'20241031'
		) ch_od_lim_a
	ORDER BY 
	    cod_acct_no, DAT_LIMIT_END DESC
) ch_od_lim
WHERE 1=1
	AND ch_od_lim.REFF = 1
	
	
--======================================================================================
	
--CREATE TEMPORARY TABLE sme_portfolio_yyyymm AS  
SELECT 
    a.*
    , CASE
        WHEN a.PRODUCT_CODE_NCBS IN ('502', '517', '502.0', '517.0', '502,0', '517,0', 'POU')
             OR a.PRODUCT_DESC LIKE '%SUPPLY CHAIN%' THEN 'Y' 
        ELSE 'N' 
    END AS ACC_FLG_DIFI,
    , CASE 
        WHEN a.FM_LINE_START_DT = DATE '1800-01-01' THEN DATE(c.DAT_LIMIT_START)
        ELSE COALESCE(a.FM_LINE_START_DT, DATE(c.DAT_LIMIT_START))
    END AS LINE_START_DT,
    , CASE 
        WHEN a.FM_MATURITY_DATE = DATE '1800-01-01' THEN DATE(c.DAT_LIMIT_END)
        ELSE COALESCE(a.fm_maturity_date, DATE(c.DAT_LIMIT_END))
    END AS LINE_MATURITY_DT
FROM 
    sme_portfolio a
LEFT JOIN 
    ch_od_lim_final c ON TRIM(a.ACCOUNT_NUMBER) = TRIM(c.COD_ACCT_NO)
ORDER BY 
    CIF
    , ACCOUNT_NUMBER
    
--============================================================================================