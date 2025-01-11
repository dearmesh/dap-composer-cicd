CREATE TEMPORARY TABLE sme_loan_limit_yyyymm AS
SELECT
    CIF
    , CREDIT_LINE_ID
    , CASE 
        WHEN PROD_GRP_TYPE NOT IN ('KAB') THEN 'not_KAB' 
        ELSE PROD_GRP_TYPE 
    END AS PROD_GROUP
    , CASE 
        WHEN PROD_GRP_TYPE IN ('KAB') THEN BALANCE_IDR 
        ELSE LIMIT_PLAFON_IDR 
    END AS LIMIT_IDR
FROM 
    dm_ba_7.sme_acc_dtl_monthly_v2
WHERE 1=1
    AND PERIOD = FORMAT_DATE('%Y%m', DATE :cursor_PROCESS_BUSINESS_DATE)
    AND ACC_TYPE_A IN ('fund_loan_active', 'loan_active')
    
--==============================================================================
    
CREATE TEMPORARY TABLE kab_loan_lim_per_cif_yyyymm AS
SELECT 
    CIF 
    , PROD_GROUP 
    , SUM(LIMIT_IDR) AS TOTAL_LIMIT
FROM 
    sme_loan_limit_yyyymm  
WHERE 1=1
    AND PROD_GROUP = 'KAB'
GROUP BY 
    CIF 
    , PROD_GROUP

--==============================================================================
    
CREATE TEMPORARY TABLE nonkab_loan_lim_per_cif_yyyymm AS
SELECT 
    CIF 
    , PROD_GROUP 
    , SUM(LIMIT_IDR) AS total_limit
FROM (
	    SELECT DISTINCT 
	        CIF 
	        , PROD_GROUP 
	        , CREDIT_LINE_ID 
	        , LIMIT_IDR
	    FROM 
	        sme_loan_limit_yyyymm  -- Gantilah yyyymm dengan nilai yang sesuai
	    WHERE 1=1
	        AND PROD_GROUP = 'not_KAB'
	) AS SUBQUERY
GROUP BY 
    CIF
    , PROD_GROUP

   
--=================================================================================
    
CREATE TEMPORARY TABLE lim_per_cif_a AS
SELECT *
FROM (
	    SELECT * 
	    FROM kab_loan_lim_per_cif_yyyymm  
	    UNION ALL
	    SELECT * 
	    FROM nonkab_loan_lim_per_cif_yyyymm  
	)
ORDER BY 
    CIF 
    , PROD_GROUP

--================================================================================
    
CREATE TEMPORARY TABLE final_lim_per_cif_yyyymm AS
SELECT 
    CIF 
    , SUM(TOTAL_LIMIT) AS TOTAL_LIMIT
FROM 
    lim_per_cif_a
GROUP BY 
    CIF


   
   
   
   
   
   
   
   
   
   
   
   
