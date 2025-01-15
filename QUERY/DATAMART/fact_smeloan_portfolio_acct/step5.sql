CREATE TEMPORARY TABLE avbal_osbal_yyyymm_per_cif AS
SELECT
    CIF
    , SUM(CASE WHEN ACC_TYPE_A = 'loan_active' THEN BALANCE_IDR END) AS TOTAL_OSBAL
    , SUM(AVBAL_LOAN_IDR) AS TOTAL_AVBAL
FROM 
    dm_ba_7.sme_acc_dtl_monthly_v2
WHERE 1=1
    AND PERIOD = FORMAT_DATE('%Y%m', DATE :cursor_PROCESS_BUSINESS_DATE)
GROUP BY 
    CIF
    
--=================================================================================
-- ================================FINAL STEP harusnya ============================
--================================= konfirmasi dengan User ========================

--CREATE TABLE fact_smeloan_portfolio_acct AS
--INSERT INTO fact_smeloan_portfolio_acct
SELECT 
	fsad.*
	, flpc.TOTAL_LIMIT
	, aoc.TOTAL_OSBAL
	, aoc.TOTAL_AVBAL
FROM final_sme_acc_dtl_yyyymm fsad
LEFT JOIN final_lim_per_cif_yyyymm flpc
ON 1=1
	AND fsad.CIF = flpc.CIF
LEFT JOIN avbal_osbal_yyyymm_per_cif aoc
ON 1=1
	AND AND fsad.CIF = aoc.CIF
WHERE 1=1