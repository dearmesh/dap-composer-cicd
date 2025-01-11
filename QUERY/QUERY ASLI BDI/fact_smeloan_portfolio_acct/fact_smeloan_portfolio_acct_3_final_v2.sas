/*================================================ FINAL V2 ================================================*/

%macro final_sme_acc_dtl(period);

/***TIME DURATION START TIME***/
%let start= %sysfunc(datetime());

options compress=yes;

%let yyyymmdd = %sysfunc(intnx(month,&period.,0,e),yymmddn8.); %put yyyymmdd:&yyyymmdd.;
%let yyyymmdd_mm1 = %sysfunc(intnx(month,&period.,-1,e),yymmddn8.); %put yyyymmdd_mm1:&yyyymmdd_mm1.;
%let yyyymmdd_mm2 = %sysfunc(intnx(month,&period.,-2,e),yymmddn8.); %put yyyymmdd_mm2:&yyyymmdd_mm2.;
%let yyyymm = %sysfunc(intnx(month,&period.,0,e),yymmn6.); %put yyyymm:&yyyymm.;
%let monyy = %sysfunc(intnx(month,&period.,0,e),monyy6.); %put monyy:&monyy.;
%let mmyy = %sysfunc(intnx(month,&period.,0,e),mmyyn4.); %put mmyy:&mmyy.;

proc sql;
create table oc_rb_dtl_&mmyy. as
select distinct
	off_code
	, branch_code_original
	, branch_name
	, region
	, position
from
/*	bdtableu.ref_oc_rb*/
/*where*/
/*	date_pr in ("&yyyymmdd.")*/
%if %sysfunc(exist(bdtableu.ref_oc_rb(where=(date_pr="&yyyymmdd."))))=1
	%then
	bdtableu.ref_oc_rb(where=(date_pr="&yyyymmdd."))
	;
%else %if %sysfunc(exist(bdtableu.ref_oc_rb(where=(date_pr="&yyyymmdd_mm1."))))=1
	%then
	bdtableu.ref_oc_rb(where=(date_pr="&yyyymmdd_mm1."))
	;
%else
	bdtableu.ref_oc_rb(where=(date_pr="&yyyymmdd_mm2."))
	;
;quit;

proc sql;
create table fm_ao_a as
select distinct
	cif
	, account_number
	, ao_business as off_code
	, acc_type_a
	, case when acc_type_a in ("fund_casa","fund_loan_active") then balance_idr else 0 end as ca_bal_idr format comma25.2
	, case when acc_type_a = "fund_td" then balance_idr else 0 end as td_bal_idr format comma25.2
	, coalesce(sum(
		(case when acc_type_a in ("fund_casa","fund_loan_active") then balance_idr end)
	, (case when acc_type_a = "fund_td" then balance_idr end)
	),0) as aum_on_idr format comma25.2
from
	sme_portfolio_&mmyy.
order by 1, 7 desc
;quit;

proc sql;
create table fm_ao_b as
select
	cif
	, account_number
	, off_code
	, aum_on_idr
from fm_ao_a
;quit;

/* ----- RANK -> this method is more like ROWNUM ----- */
proc sort data = fm_ao_b ; by cif descending aum_on_idr ;run;

data fm_ao_c;
set fm_ao_b;
by cif;
retain REFF;
format REFF 20.;
if first.cif then REFF=1;
else REFF=REFF+1;
run;
/* ----- RANK -> this method is more like ROWNUM ----- */

proc sql;
create table fm_ao_d (drop=REFF) as 
select *
from fm_ao_c
where REFF=1;
quit;

proc sql;
create table sme_ao_business as
select distinct a.cif, a.account_number

	/*account officer, position, branch code, branch name, region from finmart (no ranking process)*/
	, a.ao_business as off_cd_fm_no_rank
	, ref_ocrb_a.position as off_pos_fm_no_rank
	, ref_ocrb_a.branch_code_original as brn_cd_fm_no_rank
	, ref_ocrb_a.branch_name as brn_nm_fm_no_rank
	, ref_ocrb_a.region as region_fm_no_rank	
	
	/*account officer, position, branch code, branch name, region from finmart (manually ranked by highest aum_on)*/
	, b.off_code as off_cd_fm_highest_aum_on
	, ref_ocrb_b.position as off_pos_fm_highest_aum_on
	, ref_ocrb_b.branch_code_original as brn_cd_fm_highest_aum_on
	, ref_ocrb_b.branch_name as brn_nm_fm_highest_aum_on
	, ref_ocrb_b.region as region_fm_highest_aum_on

	/*account officer, position, branch code, branch name, region from dm_customer_profile_general_new (automatically ranked by highest aum_on)*/
	, c.off_code_highest_region_aum_on as off_cd_gnrnew_highest_aum_on
	, ref_ocrb_c.position as off_pos_gnrnew_highest_aum_on
	, ref_ocrb_c.branch_code_original as brn_cd_gnrnew_highest_aum_on
	, ref_ocrb_c.branch_name as brn_nm_gnrnew_highest_aum_on
	, ref_ocrb_c.region as region_gnrnew_highest_aum_on

from
	sme_portfolio_&mmyy. a
left join
	fm_ao_d b
		on a.cif = b.cif
left join
	cust_level_dtl_e c
		on a.cif = c.cif
left join
	oc_rb_dtl_&mmyy. ref_ocrb_a
		on trim(a.ao_business) = trim(ref_ocrb_a.off_code)
left join
	oc_rb_dtl_&mmyy. ref_ocrb_b
		on trim(b.off_code) = trim(ref_ocrb_b.off_code)
left join
	oc_rb_dtl_&mmyy. ref_ocrb_c
		on trim(c.off_code_highest_region_aum_on) = trim(ref_ocrb_c.off_code)
;quit;

proc sql;
create table lfm_&yyyymm. as
select
	input(cif_key,12.) as cif
	, account_number
	, iso_currency_cd as ccy
    , left(bdi_product_type_cd) as product format $5.
    , product_desc
    , bdi_cur_book_bal_idr as balance_idr
    , bdi_avg_book_bal_idr as avbal_loan_idr
from impbigdt.fm_acct_loan_mth
where
	date_pr = "&yyyymmdd."
	and market_segment_cd in (36,22,30,29,27,31,35)
;quit;

proc sql;
create table lfmend_summarized as
select
	cif
	, account_number
	, sum(avbal_loan_idr) as avbal_loan_per_accnum
from lfm_&yyyymm.
group by 1,2
;quit;

options compress=yes;
proc sql;
create table sme_acc_dtl_&mmyy. as
select
	&yyyymm. as period
	, a.lob_code
	, a.cif
	, a.account_number
	, a.iso_currency_cd
	, a.acc_open_date
	, a.acc_close_date
	, a.fm_ori_acc_status as fm_ori_acc_stat_cd
	, a.account_status
	, a.prod_grp_type
	, a.product_code_ncbs
	, a.product_desc
	, a.acc_flg_difi
	, a.acc_type_a
	, a.acc_type_b
	, a.data_source_a
	, a.data_source_b
	, a.credit_line_id
	, a.line_start_dt
	, a.line_maturity_dt
	, a.line_status as line_stat_cd
	, a.line_stat_desc as line_status
	, a.balance_idr
	, d.avbal_loan_per_accnum as avbal_loan_idr

	, a.limit_plafon_idr
	, a.rate
	, a.amount_hold
	, a.collectibility

	, b.flg_funding_cust
	, b.flg_lending_cust
	, b.flg_both_fl_cust
	, b.customer_type
	, b.flg_bpr
	, b.flg_kopkar
	, b.zip_code
	, b.business_segment
	, b.cust_birth_year
	, b.cust_gender
	, b.cust_education
	, b.cust_profession
	, b.cust_income
	, b.homebrn_name
	, b.cust_religion
	, b.cust_marital
	, b.no_dependent
	, b.flg_staff
	, b.mob
	, b.mob_dbankpro
	, b.acquisition_channel_level_1
	, b.acquisition_channel_level_2
	, b.acquisition_channel_level_3
	, b.acquisition_channel_level_4

	/*account officer, position, branch code, branch name, region from finmart (no ranking process)*/
	, c.off_cd_fm_no_rank
	, c.off_pos_fm_no_rank
	, c.brn_cd_fm_no_rank
	, c.brn_nm_fm_no_rank
	, c.region_fm_no_rank	
	
	/*account officer, position, branch code, branch name, region from finmart (manually ranked by highest aum_on)*/
	, c.off_cd_fm_highest_aum_on
	, c.off_pos_fm_highest_aum_on
	, c.brn_cd_fm_highest_aum_on
	, c.brn_nm_fm_highest_aum_on
	, c.region_fm_highest_aum_on

	/*account officer, position, branch code, branch name, region from dm_customer_profile_general_new (automatically ranked by highest aum_on)*/
	, c.off_cd_gnrnew_highest_aum_on
	, c.off_pos_gnrnew_highest_aum_on
	, c.brn_cd_gnrnew_highest_aum_on
	, c.brn_nm_gnrnew_highest_aum_on
	, c.region_gnrnew_highest_aum_on

	, b.previous_month_segment_volume
	, b.previous_month_segment_flag
	, b.customer_segment_by_volume
	, b.customer_segment_by_flag

	, b.sum_neg_ca_osbal_idr
	, b.sum_mis_neg_avg_ca
	, b.sum_fin_neg_avg_ca

	, b.sum_pos_ca_osbal_idr
	, b.sum_mis_pos_avg_ca
	, b.sum_fin_pos_avg_ca

	, b.sum_neg_sa_osbal_idr
	, b.sum_mis_neg_avg_sa
	, b.sum_fin_neg_avg_sa

	, b.sum_pos_sa_osbal_idr
	, b.sum_mis_pos_avg_sa
	, b.sum_fin_pos_avg_sa

	, b.sum_osbal_td
	, b.sum_aum_off

	, b.have_active_dbankpro
	, b.have_active_dcc	
from
	sme_portfolio_&mmyy. a
left join
	cust_level_dtl_e b
		on a.cif = b.cif
left join
	sme_ao_business c
		on a.cif = c.cif
		and trim(a.account_number) = trim(c.account_number)
left join
	lfmend_summarized d
		on trim(a.account_number) = trim(d.account_number)
;quit;

proc sql;
create table final_sme_acc_dtl_&mmyy. as
select distinct * from sme_acc_dtl_&mmyy.
;quit;

/***TIME DURATION END TIME***/
%let end=%sysfunc(datetime());
%let duration=%sysfunc(putn(%sysevalf(&end-&start),time.));
%put timecost &duration.;

%mend final_sme_acc_dtl;

%final_sme_acc_dtl(&master_period.);