/*============================================================================================*/
/*================================================ CUST INFO ==================================*/
/*============================================================================================*/


--CREATE TEMPORARY TABLE cif_list_funding AS
SELECT DISTINCT CIF
FROM sme_portfolio_yyyymm  
WHERE 1=1
	AND ACC_TYPE_A IN ('fund_casa', 'fund_loan_active', 'fund_td')
ORDER BY cif;

--====================================================================

--CREATE TEMPORARY TABLE cif_list_lending AS
SELECT DISTINCT cif
FROM sme_portfolio_yyyymm  
WHERE ACC_TYPE_A IN ('loan_active', 'fund_loan_active')
ORDER BY cif;

--====================================================================

--CREATE TEMPORARY TABLE cif_list_both_fl AS
SELECT DISTINCT a.CIF
FROM cif_list_funding a
	, cif_list_lending b 
WHERE 1=1
	AND a.CIF = b.CIF
ORDER BY a.CIF;

--====================================================================

--CREATE TEMPORARY TABLE cust_level_dtl_a AS
SELECT DISTINCT
    a.CIF,
    CASE WHEN b.CIF IS NULL THEN 0 ELSE 1 END AS FLG_FUNDING_CUST,
    CASE WHEN c.CIF IS NULL THEN 0 ELSE 1 END AS FLG_LENDING_CUST,
    CASE WHEN d.CIF IS NULL THEN 0 ELSE 1 END AS FLG_BOTH_FL_CUST
FROM (
	    SELECT DISTINCT CIF 
	    FROM sme_portfolio_yyyymm 
	) a
LEFT JOIN cif_list_funding b 
ON 1=1
	AND a.CIF = b.CIF
LEFT JOIN cif_list_lending c 
ON 1=1
	AND a.CIF = c.CIF
LEFT JOIN cif_list_both_fl d
ON 1=1
	AND a.CIF = d.CIF


--=========================================================================

--CREATE TEMPORARY TABLE cust_level_dtl_b AS
SELECT DISTINCT
    a.*
    , CASE 
        WHEN b.FLG_CUST_TYP IN ('A', 'B') THEN 'PERSONAL' 
        ELSE 'NON-PERSONAL' 
    END AS CUSTOMER_TYPE
    , CASE
        WHEN LOWER(b.NAM_CUST_FULL) LIKE '%bpr%' OR 
             LOWER(b.NAM_CUST_FULL) LIKE '%bank perkreditan%' OR 
             LOWER(b.NAM_CUST_FULL) LIKE '%bank pembiayaan%' THEN 'Y' 
        ELSE 'N' 
    END AS FLG_BPR
    , CASE
        WHEN LOWER(b.NAM_CUST_FULL) LIKE '%kopkar%' OR 
             LOWER(b.NAM_CUST_FULL) LIKE '%koperasi%' OR 
             LOWER(b.NAM_CUST_FULL) LIKE '%kopeg%' OR 
             LOWER(b.NAM_CUST_FULL) LIKE '%koptan%' OR 
             LOWER(b.NAM_CUST_FULL) LIKE '%koperta%' OR 
             LOWER(b.NAM_CUST_FULL) LIKE '%kpri%' THEN 'Y' 
        ELSE 'N' 
    END AS FLG_KOPKAR
    , b.TXT_CUSTADR_ZIP AS ZIP_CODE
    , b.TXT_BUSINESS_TYP AS BUSINESS_SEGMENT
FROM 
    cust_level_dtl_a a
LEFT JOIN ( SELECT *
			FROM MISPLUS.BD_CI_CUSTMAST b
		  	WHERE 1=1
--		  		AND b.DATE_PR = FORMAT_DATE('%Y%m%d', DATE :cursor_PROCESS_BUSINESS_DATE) --'20240625'
		  ) b
ON 1=1
	AND a.CIF = CAST(b.COD_CUST_ID AS STRING)
WHERE 1=1

--====================================================================================================

--CREATE TEMPORARY TABLE cust_level_dtl_c AS
SELECT
    a.*
    , CAST(B.CUST_BIRTH_YEAR AS INT64) AS CUST_BIRTH_YEAR
    , b.CUST_GENDER
    , b.CUST_EDUCATION
    , b.CUST_PROFESSION
    , CASE
        WHEN b.CUSTINCOME IN ('P  - < RP.500 Ribu', 
                              'P  -   Rp.500 Ribu - Rp.  1 Juta', 
                              'NP - < Rp.  1 Juta') THEN 'cust_income < Rp 1 Mio'
        WHEN b.CUSTINCOME IN ('NP -   Rp.  1 Juta - Rp. 10 Juta', 
                              'P  - > Rp.  1 Juta - Rp.  5 Juta', 
                              'P  - > Rp.  5 Juta - Rp. 10 Juta') THEN 'Rp 1 Mio =< cust_income < Rp 10 Mio'
        WHEN b.CUSTINCOME IN ('NP - > Rp. 10 Juta - Rp. 25 Juta', 
                              'P  - > Rp. 10 Juta - Rp. 25 Juta') THEN 'Rp 10 Mio =< cust_income < Rp 25 Mio'
        WHEN b.CUSTINCOME IN ('NP - > RP. 25 Juta - Rp. 50 Juta', 
                              'P  - > Rp. 25 Juta - Rp. 50 Juta') THEN 'Rp 25 Mio =< cust_income < Rp 50 Mio'
        WHEN b.CUSTINCOME IN ('NP - > RP. 50 Juta - Rp.100 Juta', 
                              'P  - > Rp. 50 Juta - Rp.100 Juta') THEN 'Rp 50 Mio =< cust_income < Rp 100 Mio'
        WHEN b.CUSTINCOME IN ('NP - > RP.100 Juta - Rp.500 Juta', 
                              'NP - > RP.500 Juta - Rp.  1 Miliar', 
                              'NP - > Rp.  1 Miliar', 
                              'P  - > Rp.100 Juta') THEN '> Rp 100 Mio'
        WHEN b.CUSTINCOME IS NULL THEN 'N/A'
        ELSE b.CUSTINCOME
    END AS CUST_INCOME
    , b.HOMEBRN_NAME
    , b.CUST_RELIGION
    , b.CUST_MARITAL
    , CAST(b.NO_DEPENDENT AS INT64) AS NO_DEPENDENT
    , b.FLG_STAFF
    -- , b.MOB
    , CAST(b.MOB_DBANKPRO AS INT64) AS MOB_DBANKPRO
    , b.ACQUISITION_CHANNEL_LEVEL_1
    , b.ACQUISITION_CHANNEL_LEVEL_2
    , b.ACQUISITION_CHANNEL_LEVEL_3
    , b.ACQUISITION_CHANNEL_LEVEL_4
    , b.OFF_CODE_HIGHEST_REGION_AUM_ON
    , b.PREVIOUS_MONTH_SEGMENT_VOLUME
    , b.PREVIOUS_MONTH_SEGMENT_FLAG
    , b.CUSTOMER_SEGMENT_BY_VOLUME
    , b.CUSTOMER_SEGMENT_BY_FLAG
    , b.OSBAL_TD AS SUM_OSBAL_TD
    , b.AUM_OFF AS SUM_AUM_OFF
    , CASE 
	    WHEN b.HAVE_ACTIVE_DBANKPRO IS NULL THEN 0 
	    ELSE 1 
	  END AS HAVE_ACTIVE_DBANKPRO
FROM 
    cust_level_dtl_b a
LEFT JOIN ( 
    		SELECT *
    		FROM DM_DAP.DM_CUSTOMER_PROFILE_GENERAL_NEW b 
    		WHERE 1=1
--		  		AND b.DATE_PR = FORMAT_DATE('%Y%m%d', DATE :cursor_PROCESS_BUSINESS_DATE) --'20240101'
		) b
ON 1=1
	AND a.CIF = CAST(b.CIF AS STRING)
WHERE 1=1

/*============================================================================================*/
/*=====================================TRUE CASA BALANCE======================================*/
/*============================================================================================*/

--CREATE TEMPORARY TABLE dm_casa_funding_yyyymm AS
SELECT DISTINCT
    b.cif
    , a.FLG_FUNDING_CUST
    , a.FLG_LENDING_CUST
    , a.FLG_BOTH_FL_CUST
    , b.COD_ACCT_NO
    , b.ACCT_STATUS_GROUP
    , b.PROD_GROUP_LEVEL
    , b.BALANCE_IDR
    , b.MIS_AVERAGE_BALANCE_IDR
    , b.FIN_AVERAGE_BALANCE_IDR
FROM 
    cust_level_dtl_c a 
     DM_DAP.DM_CASA_FUNDING b 
WHERE 1=1
	AND a.CIF = CAST(b.CIF AS STRING)
    AND b.ACCT_STATUS_GROUP <> 'CLOSED'
--	AND b.DATE_PR = FORMAT_DATE('%Y%m%d', DATE :cursor_PROCESS_BUSINESS_DATE) --'20230331'
    

 --================================================================================================
    
--CREATE TEMPORARY TABLE fund_acc_smmry_yyyymm AS
SELECT
    cif
    , SUM(CASE WHEN BALANCE_IDR < 0 AND PROD_GROUP_LEVEL = 'CA' THEN BALANCE_IDR END) AS SUM_NEG_CA_OSBAL_IDR
    , SUM(CASE WHEN MIS_AVERAGE_BALANCE_IDR < 0 AND PROD_GROUP_LEVEL = 'CA' THEN MIS_AVERAGE_BALANCE_IDR END) AS SUM_MIS_NEG_AVG_CA
    , SUM(CASE WHEN FIN_AVERAGE_BALANCE_IDR < 0 AND PROD_GROUP_LEVEL = 'CA' THEN FIN_AVERAGE_BALANCE_IDR END) AS SUM_FIN_NEG_AVG_CA
    , SUM(CASE WHEN BALANCE_IDR < 0 AND PROD_GROUP_LEVEL = 'SA' THEN BALANCE_IDR END) AS SUM_NEG_SA_OSBAL_IDR
    , SUM(CASE WHEN MIS_AVERAGE_BALANCE_IDR < 0 AND PROD_GROUP_LEVEL = 'SA' THEN MIS_AVERAGE_BALANCE_IDR END) AS SUM_MIS_NEG_AVG_SA
    , SUM(CASE WHEN FIN_AVERAGE_BALANCE_IDR < 0 AND PROD_GROUP_LEVEL = 'SA' THEN FIN_AVERAGE_BALANCE_IDR END) AS SUM_FIN_NEG_AVG_SA
    , SUM(CASE WHEN (BALANCE_IDR = 0 OR BALANCE_IDR > 0) AND PROD_GROUP_LEVEL = 'CA' THEN BALANCE_IDR END) AS SUM_POS_CA_OSBAL_IDR
    , SUM(CASE WHEN (MIS_AVERAGE_BALANCE_IDR = 0 OR MIS_AVERAGE_BALANCE_IDR > 0) AND PROD_GROUP_LEVEL = 'CA' THEN MIS_AVERAGE_BALANCE_IDR END) AS SUM_MIS_POS_AVG_CA
    , SUM(CASE WHEN (FIN_AVERAGE_BALANCE_IDR = 0 OR FIN_AVERAGE_BALANCE_IDR > 0) AND PROD_GROUP_LEVEL = 'CA' THEN FIN_AVERAGE_BALANCE_IDR END) AS SUM_FIN_POS_AVG_CA
    , SUM(CASE WHEN (BALANCE_IDR = 0 OR BALANCE_IDR > 0) AND PROD_GROUP_LEVEL = 'SA' THEN BALANCE_IDR END) AS SUM_POS_SA_OSBAL_IDR
    , SUM(CASE WHEN (MIS_AVERAGE_BALANCE_IDR = 0 OR MIS_AVERAGE_BALANCE_IDR > 0) AND PROD_GROUP_LEVEL = 'SA' THEN MIS_AVERAGE_BALANCE_IDR END) AS SUM_MIS_POS_AVG_SA
    , SUM(CASE WHEN (FIN_AVERAGE_BALANCE_IDR = 0 OR FIN_AVERAGE_BALANCE_IDR > 0) AND PROD_GROUP_LEVEL = 'SA' THEN FIN_AVERAGE_BALANCE_IDR END) AS SUM_FIN_POS_AVG_SA
FROM 
    dm_casa_funding_yyyymm  
GROUP BY 
    cif
    
--=====================================================================================================
    
--CREATE TEMPORARY TABLE cust_level_dtl_d AS
SELECT
    a.*,
    , b.SUM_NEG_CA_OSBAL_IDR
    , b.SUM_MIS_NEG_AVG_CA
    , b.SUM_FIN_NEG_AVG_CA
    , b.SUM_POS_CA_OSBAL_IDR
    , b.SUM_MIS_POS_AVG_CA
    , b.SUM_FIN_POS_AVG_CA
    , b.SUM_NEG_SA_OSBAL_IDR
    , b.SUM_MIS_NEG_AVG_SA
    , b.SUM_FIN_NEG_AVG_SA
    , b.SUM_POS_SA_OSBAL_IDR
    , b.SUM_MIS_POS_AVG_SA
    , b.SUM_FIN_POS_AVG_SA
FROM cust_level_dtl_c AS a
LEFT JOIN fund_acc_smmry_yyyymm AS b 
ON 1=1
	AND a.CIF = CAST(b.CIF AS STRING)
	
/*====================================================================================*/
/*===================================== DCC REG ======================================*/
/*====================================================================================*/

--CREATE TEMPORARY TABLE cust_level_dtl_e AS
SELECT
    a.*
    , CASE 
	    WHEN b.CIF IS NOT NULL THEN 1 
	    ELSE 0 
	  END AS HAVE_ACTIVE_DCC
FROM 
    cust_level_dtl_d AS a
LEFT JOIN (
			    SELECT DISTINCT
			        HOST_CIF_ID AS cif
			    FROM 
			        MISPLUS.BD_DCC_PCC_CORP 
			    WHERE 1=1
			        --	AND b.DATE_PR = FORMAT_DATE('%Y%m%d', DATE :cursor_PROCESS_BUSINESS_DATE) --'20241201'
			        AND TRIM(IS_DELETE) = 'N'
			) b 
ON 1=1
	AND a.CIF = b.CIF
	
