/*================================================ FINAL V2 ================================================*/
BEGIN
    DECLARE v_data_count INT64;

    -- Cek apakah ada data untuk input_date_pr - 0
    SET v_data_count = (
					        SELECT COUNT(1)
					        FROM DM_DAP.REF_OC_RB ror
					        WHERE 1=1
					        	--	AND ror.DATE_PR = FORMAT_DATE('%Y%m%d', DATE :cursor_PROCESS_BUSINESS_DATE) --'20241201'
					    );

    IF v_data_count > 0 THEN
    	-- EXECUTE IMMEDIATE
        CREATE TEMPORARY TABLE oc_rb_dtl_yyyymm AS
		SELECT DISTINCT
		    off_code,
		    branch_code_original,
		    branch_name,
		    region,
		    position
		FROM 
		    DM_DAP.REF_OC_RB ror
		WHERE 1=1
		    --	AND ror.DATE_PR = FORMAT_DATE('%Y%m%d', DATE :cursor_PROCESS_BUSINESS_DATE) --'20241201'
    ELSE
    	-- Cek apakah ada data untuk input_date_pr - 1
        SET v_data_count = (
						        SELECT COUNT(1)
						        FROM DM_DAP.REF_OC_RB ror
						        WHERE 1=1
						        	--	AND ror.DATE_PR = DATE_SUB(FORMAT_DATE('%Y%m%d', DATE :cursor_PROCESS_BUSINESS_DATE), INTERVAL 1 MONTH) --'20241201'
						    );

        IF v_data_count > 0 THEN
            -- EXECUTE IMMEDIATE
	        CREATE TEMPORARY TABLE oc_rb_dtl_yyyymm AS
			SELECT DISTINCT
			    off_code,
			    branch_code_original,
			    branch_name,
			    region,
			    position
			FROM 
			    DM_DAP.REF_OC_RB ror
			WHERE 1=1
			    --	AND ror.DATE_PR = DATE_SUB(FORMAT_DATE('%Y%m%d', DATE :cursor_PROCESS_BUSINESS_DATE), INTERVAL 1 MONTH) --'20241201'
        ELSE
        	-- Gunakan data untuk input_date_pr - 2
            -- EXECUTE IMMEDIATE
	        CREATE TEMPORARY TABLE oc_rb_dtl_yyyymm AS
			SELECT DISTINCT
			    off_code,
			    branch_code_original,
			    branch_name,
			    region,
			    position
			FROM 
			    DM_DAP.REF_OC_RB ror
			WHERE 1=1
			    --	AND ror.DATE_PR = DATE_SUB(FORMAT_DATE('%Y%m%d', DATE :cursor_PROCESS_BUSINESS_DATE), INTERVAL 2 MONTH) --'20241201'
    
        END IF;
    END IF;
END;

--==================================================================================================================================

--CREATE TEMPORARY TABLE fm_ao_a AS
CREATE TEMPORARY TABLE fm_ao_a AS
    SELECT 
      sub.CIF
      , sub.ACCOUNT_NUMBER
      , sub.OFF_CODE
      , sub.ACC_TYPE_A
      , COALESCE(SUM(sub.BAL_IDR_1) + sum(sub.BAL_IDR_2),0) AUM_ON_IDR
    FROM
    (
      SELECT DISTINCT
          CIF
          , ACCOUNT_NUMBER
          , AO_BUSINESS AS OFF_CODE
          , ACC_TYPE_A
          , CASE 
              WHEN ACC_TYPE_A IN ('fund_casa', 'fund_loan_active') THEN BALANCE_IDR 
              ELSE 0 
          END AS CA_BAL_IDR
          , CASE 
              WHEN ACC_TYPE_A = 'fund_td' THEN BALANCE_IDR 
              ELSE 0 
          END AS TD_BAL_IDR
          , CASE 
              WHEN ACC_TYPE_A IN ('fund_casa', 'fund_loan_active') THEN BALANCE_IDR 
              ELSE 0
            END BAL_IDR_1
          , CASE 
              WHEN ACC_TYPE_A = 'fund_td' THEN BALANCE_IDR 
              ELSE 0
            END BAL_IDR_2 
      FROM 
          sme_portfolio_yyyymm
    ) sub 
    WHERE 1=1
    GROUP BY 
        sub.CIF
      , sub.ACCOUNT_NUMBER
      , sub.OFF_CODE
      , sub.ACC_TYPE_A
    ORDER BY 
        sub.CIF
      , AUM_ON_IDR DESC
   
--=======================================================================================
   
--CREATE TEMPORARY TABLE fm_ao_b AS
SELECT
    CIF
    , ACCOUNT_NUMBER
    , OFF_CODE
    , AUM_ON_IDR
FROM 
    fm_ao_a
ORDER BY 
    CIF
   , AUM_ON_IDR DESC
    
--================================================================================
    
--CREATE TEMPORARY TABLE fm_ao_c AS
--SELECT 
--	*
--    , ROW_NUMBER() OVER (PARTITION BY CIF ORDER BY AUM_ON_IDR DESC) AS REFF
--FROM 
--	fm_ao_b
--WHERE 1=1

-- PINDAH KE PALING BAWAH   
   
--===============================================================================

--CREATE TEMPORARY TABLE fm_ao_d AS
SELECT fm_ao_c.*
FROM 
	(
		SELECT 
			*
		    , ROW_NUMBER() OVER (PARTITION BY CIF ORDER BY AUM_ON_IDR DESC) AS REFF
		FROM 
			fm_ao_b
		WHERE 1=1
	) fm_ao_c
WHERE 1=1
	AND REFF = 1
   
--================================================================================
	
--CREATE TEMPORARY TABLE sme_ao_business AS
SELECT DISTINCT 
    a.CIF
    , a.ACCOUNT_NUMBER
    /* Account officer, position, branch code, branch name, region from finmart (no ranking process) */
    , a.AO_BUSINESS AS OFF_CD_FM_NO_RANK
    , ref_ocrb_a.POSITION AS OFF_POS_FM_NO_RANK
    , ref_ocrb_a.BRANCH_CODE_ORIGINAL AS BRN_CD_FM_NO_RANK
    , ref_ocrb_a.BRANCH_NAME AS BRN_NM_FM_NO_RANK
    , ref_ocrb_a.REGION AS REGION_FM_NO_RANK
    /* Account officer, position, branch code, branch name, region from finmart (manually ranked by highest aum_on) */
    , b.OFF_CODE AS OFF_CD_FM_HIGHEST_AUM_ON
    , ref_ocrb_b.POSITION AS OFF_POS_FM_HIGHEST_AUM_ON
    , ref_ocrb_b.BRANCH_CODE_ORIGINAL AS BRN_CD_FM_HIGHEST_AUM_ON
    , ref_ocrb_b.BRANCH_NAME AS BRN_NM_FM_HIGHEST_AUM_ON
    , ref_ocrb_b.REGION AS REGION_FM_HIGHEST_AUM_ON
    /* Account officer, position, branch code, branch name, region from dm_customer_profile_general_new (automatically ranked by highest aum_on) */
    , c.OFF_CODE_HIGHEST_REGION_AUM_ON AS OFF_CD_GNRNEW_HIGHEST_AUM_ON
    , ref_ocrb_c.POSITION AS OFF_POS_GNRNEW_HIGHEST_AUM_ON
    , ref_ocrb_c.BRANCH_CODE_ORIGINAL AS BRN_CD_GNRNEW_HIGHEST_AUM_ON
    , ref_ocrb_c.BRANCH_NAME AS BRN_NM_GNRNEW_HIGHEST_AUM_ON
    , ref_ocrb_c.REGION AS REGION_GNRNEW_HIGHEST_AUM_ON
FROM 
    sme_portfolio_yyyymm a  
LEFT JOIN fm_ao_d b 
ON 1=1
	AND a.CIF = b.CIF
LEFT JOIN cust_level_dtl_e c 
ON 1=1
	AND a.CIF = c.CIF
LEFT JOIN oc_rb_dtl_yyyymm ref_ocrb_a 
ON 1=1 
	AND TRIM(a.AO_BUSINESS) = TRIM(ref_ocrb_a.OFF_CODE)
LEFT JOIN oc_rb_dtl_yyyymm ref_ocrb_b 
ON 1=1
	AND TRIM(b.OFF_CODE) = TRIM(ref_ocrb_b.OFF_CODE)
LEFT JOIN oc_rb_dtl_yyyymm ref_ocrb_c 
ON 1=1
	AND TRIM(c.OFF_CODE_HIGHEST_REGION_AUM_ON) = TRIM(ref_ocrb_c.OFF_CODE)
	
--===========================================================================
	
--CREATE TEMPORARY TABLE lfm_yyyymm AS
SELECT
    SAFE_CAST(CIF_KEY AS INT64) AS CIF  -- Mengonversi cif_key menjadi INT64
    , ACCOUNT_NUMBER
    , ISO_CURRENCY_CD AS CCY
    , LEFT(BDI_PRODUCT_TYPE_CD, 5) AS PRODUCT  -- Mengambil 5 karakter pertama dari bdi_product_type_cd
    , PRODUCT_DESC
    , BDI_CUR_BOOK_BAL_IDR AS BALANCE_IDR
    , BDI_AVG_BOOK_BAL_IDR AS AVBAL_LOAN_IDR
FROM 
    MISPLUS.FM_ACCT_LOAN_MTH
WHERE 1=1
--    AND DATE_PR = FORMAT_DATE('%Y%m%d', DATE :cursor_PROCESS_BUSINESS_DATE) --'20241201'
    AND MARKET_SEGMENT_CD IN (36, 22, 30, 29, 27, 31, 35);

--=============================================================================
   
--CREATE TEMPORARY TABLE lfmend_summarized AS
SELECT
    CIF
    , ACCOUNT_NUMBER
    , SUM(AVBAL_LOAN_IDR) AS AVBAL_LOAN_PER_ACCNUM
FROM 
    lfm_yyyymm
GROUP BY 
    CIF
    , ACCOUNT_NUMBER
    
--===============================================================================

--CREATE TEMPORARY TABLE sme_acc_dtl_yyyymm AS
SELECT
    FORMAT_DATE('%Y%m', DATE :cursor_PROCESS_BUSINESS_DATE) AS PERIOD
    , a.LOB_CODE
    , a.CIF
    , a.ACCOUNT_NUMBER
    , a.ISO_CURRENCY_CD
    , a.ACC_OPEN_DATE
    , a.ACC_CLOSE_DATE
    , a.FM_ORI_ACC_STATUS AS FM_ORI_ACC_STAT_CD
    , a.ACCOUNT_STATUS
    , a.PROD_GRP_TYPE
    , a.PRODUCT_CODE_NCBS
    , a.PRODUCT_DESC
    , a.ACC_FLG_DIFI
    , a.ACC_TYPE_A
    , a.ACC_TYPE_B
    , a.DATA_SOURCE_A
    , a.DATA_SOURCE_B
    , a.CREDIT_LINE_ID
    , a.LINE_START_DT
    , a.LINE_MATURITY_DT
    , a.LINE_STATUS AS LINE_STAT_CD
    , a.LINE_STAT_DESC AS LINE_STATUS
    , a.BALANCE_IDR
    , d.AVBAL_LOAN_PER_ACCNUM AS AVBAL_LOAN_IDR
    , a.LIMIT_PLAFON_IDR
    , a.RATE
    , a.AMOUNT_HOLD
    , a.COLLECTIBILITY
    , b.FLG_FUNDING_CUST
    , b.FLG_LENDING_CUST
    , b.FLG_BOTH_FL_CUST
    , b.CUSTOMER_TYPE
    , b.FLG_BPR
    , b.FLG_KOPKAR
    , b.ZIP_CODE
    , b.BUSINESS_SEGMENT
    , b.CUST_BIRTH_YEAR
    , b.CUST_GENDER
    , b.CUST_EDUCATION
    , b.CUST_PROFESSION
    , b.CUST_INCOME
    , b.HOMEBRN_NAME
    , b.CUST_RELIGION
    , b.CUST_MARITAL
    , b.NO_DEPENDENT
    , b.FLG_STAFF
    , b.MOB
    , b.MOB_DBANKPRO
    , b.ACQUISITION_CHANNEL_LEVEL_1
    , b.ACQUISITION_CHANNEL_LEVEL_2
    , b.ACQUISITION_CHANNEL_LEVEL_3
    , b.ACQUISITION_CHANNEL_LEVEL_4
    /* Account officer, position, branch code, branch name, region from finmart (no ranking process) */
    , c.OFF_CD_FM_NO_RANK
    , c.OFF_POS_FM_NO_RANK
    , c.BRN_CD_FM_NO_RANK
    , c.BRN_NM_FM_NO_RANK
    , c.REGION_FM_NO_RANK
    /* Account officer, position, branch code, branch name, region from finmart (manually ranked by highest aum_on) */
    , c.OFF_CD_FM_HIGHEST_AUM_ON
    , c.OFF_POS_FM_HIGHEST_AUM_ON
    , c.BRN_CD_FM_HIGHEST_AUM_ON
    , c.BRN_NM_FM_HIGHEST_AUM_ON
    , c.REGION_FM_HIGHEST_AUM_ON
    /* Account officer, position, branch code, branch name, region from dm_customer_profile_general_new (automatically ranked by highest aum_on) */
    , c.OFF_CD_GNRNEW_HIGHEST_AUM_ON
    , c.OFF_POS_GNRNEW_HIGHEST_AUM_ON
    , c.BRN_CD_GNRNEW_HIGHEST_AUM_ON
    , c.BRN_NM_GNRNEW_HIGHEST_AUM_ON
    , c.REGION_GNRNEW_HIGHEST_AUM_ON
    , b.PREVIOUS_MONTH_SEGMENT_VOLUME
    , b.PREVIOUS_MONTH_SEGMENT_FLAG
    , b.CUSTOMER_SEGMENT_BY_VOLUME
    , b.CUSTOMER_SEGMENT_BY_FLAG
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
    , b.SUM_OSBAL_TD
    , b.SUM_AUM_OFF
    , b.HAVE_ACTIVE_DBANKPRO
    , b.HAVE_ACTIVE_DCC
FROM
    sme_portfolio_yyyymm a  -- Gantilah yyyymm dengan nilai yang sesuai
LEFT JOIN cust_level_dtl_e b 
ON 1=1
	AND a.cif = b.cif
LEFT JOIN sme_ao_business c 
ON 1=1
	AND a.cif = c.cif AND TRIM(a.account_number) = TRIM(c.account_number)
LEFT JOIN lfmend_summarized d 
ON 1=1
	AND TRIM(a.account_number) = TRIM(d.account_number)

--==========================================================================

CREATE TEMPORARY TABLE final_sme_acc_dtl_yyyymm AS
SELECT DISTINCT *
FROM sme_acc_dtl_yyyymm


