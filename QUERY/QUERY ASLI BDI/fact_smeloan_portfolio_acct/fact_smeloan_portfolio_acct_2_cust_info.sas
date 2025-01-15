/*================================================ CUST INFO ================================================*/

%macro sme_cust(period);

/***TIME DURATION START TIME***/
%let start= %sysfunc(datetime());

options compress=yes;

%let yyyymmdd = %sysfunc(intnx(month,&period.,0,e),yymmddn8.); %put yyyymmdd:&yyyymmdd.;
%let yyyymmdd_b = %sysfunc(intnx(month,&period.,0,b),yymmddn8.); %put yyyymmdd_b:&yyyymmdd_b.;
%let yyyymm = %sysfunc(intnx(month,&period.,0,e),yymmn6.); %put yyyymm:&yyyymm.;
%let monyy = %sysfunc(intnx(month,&period.,0,e),monyy6.); %put monyy:&monyy.;
%let mmyy = %sysfunc(intnx(month,&period.,0,e),mmyyn4.); %put mmyy:&mmyy.;

proc sql;
create table cif_list_funding as
select distinct cif as cif from sme_portfolio_&mmyy.
where acc_type_a in ("fund_casa","fund_loan_active","fund_td")
order by 1
;quit;

proc sql;
create table cif_list_lending as
select distinct cif as cif from sme_portfolio_&mmyy.
where acc_type_a in ("loan_active","fund_loan_active")
order by 1
;quit;

proc sql;
create table cif_list_both_fl as
select distinct a.cif as cif
from cif_list_funding a
inner join cif_list_lending b
on a.cif = b.cif
order by 1
;quit;

proc sql;
create table cust_level_dtl_a as
select distinct
	a.cif
	, case when b.cif is null then 0 else 1 end as flg_funding_cust
	, case when c.cif is null then 0 else 1 end as flg_lending_cust
	, case when d.cif is null then 0 else 1 end as flg_both_fl_cust
from (select distinct cif from sme_portfolio_&mmyy.) a
left join cif_list_funding b on a.cif = b.cif
left join cif_list_lending c on a.cif = c.cif
left join cif_list_both_fl d on a.cif = d.cif
;quit;

proc sql;
create table cust_level_dtl_b as
select distinct
	a.*
	, case when b.flg_cust_typ in ('A','B') then 'PERSONAL' else 'NON-PERSONAL' end as customer_type
	, case
		when lower(b.nam_cust_full) like "%bpr%"
			or lower(b.nam_cust_full) like "%bank perkreditan%"
			or lower(b.nam_cust_full) like "%bank pembiayaan%"
				then "Y" else "N" end as flg_bpr
	, case
		when lower(b.nam_cust_full) like "%kopkar%"
			or lower(b.nam_cust_full) like "%koperasi%"
			or lower(b.nam_cust_full) like "%kopeg%"
			or lower(b.nam_cust_full) like "%koptan%"
			or lower(b.nam_cust_full) like "%koperta%"
			or lower(b.nam_cust_full) like "%kpri%"
				then "Y" else "N" end as flg_kopkar	
	, b.txt_custadr_zip as zip_code
	, b.txt_business_typ as business_segment
from
	cust_level_dtl_a a
left join
	impbigdt.bd_ci_custmast(where=(date_pr="&yyyymmdd.")) b
		on a.cif = b.cod_cust_id
;quit;

proc sql;
create table cust_level_dtl_c as
select
	a.*
	, input(b.cust_birth_year,12.) as cust_birth_year
	, b.cust_gender
	, b.cust_education
	, b.cust_profession
	, case
		when
			b.custincome = 'P  - < RP.500 Ribu'
			or b.custincome = 'P  -   Rp.500 Ribu - Rp.  1 Juta'
    		or b.custincome = 'NP - < Rp.  1 Juta'
				then 'cust_income < Rp 1 Mio'
		when
			b.custincome = 'NP -   Rp.  1 Juta - Rp. 10 Juta'
			or b.custincome = 'P  - > Rp.  1 Juta - Rp.  5 Juta'
    		or b.custincome = 'P  - > Rp.  5 Juta - Rp. 10 Juta'
				then 'Rp 1 Mio =< cust_income < Rp 10 Mio'
		when
			b.custincome = 'NP - > Rp. 10 Juta - Rp. 25 Juta'
			or b.custincome = 'P  - > Rp. 10 Juta - Rp. 25 Juta'
				then 'Rp 10 Mio =< cust_income < Rp 25 Mio'
		when
			b.custincome = 'NP - > RP. 25 Juta - Rp. 50 Juta'
			or b.custincome = 'P  - > Rp. 25 Juta - Rp. 50 Juta'
				then 'Rp 25 Mio =< cust_income < Rp 50 Mio'
		when
			b.custincome = 'NP - > RP. 50 Juta - Rp.100 Juta'
			or b.custincome = 'P  - > Rp. 50 Juta - Rp.100 Juta'
				then 'Rp 50 Mio =< cust_income < Rp 100 Mio'
		when
			b.custincome = 'NP - > RP.100 Juta - Rp.500 Juta' 
    		or b.custincome = 'NP - > RP.500 Juta - Rp.  1 Miliar'
    		or b.custincome = 'NP - > Rp.  1 Miliar'
    		or b.custincome = 'P  - > Rp.100 Juta'
				then '> Rp 100 Mio'
		when b.custincome is null then "N/A"
		else b.custincome
	end as cust_income
	, b.homebrn_name
	, b.cust_religion
	, b.cust_marital
	, input(b.no_dependent,12.) as no_dependent
	, b.flg_staff
	, b.mob
	, input(b.mob_dbankpro,12.) as mob_dbankpro
	, b.acquisition_channel_level_1
	, b.acquisition_channel_level_2
	, b.acquisition_channel_level_3
	, b.acquisition_channel_level_4
	, b.off_code_highest_region_aum_on
	, b.previous_month_segment_volume
	, b.previous_month_segment_flag
	, b.customer_segment_by_volume
	, b.customer_segment_by_flag
/*	, b.highest_region_aum_on*/
	, b.osbal_td as sum_osbal_td
	, b.aum_off as sum_aum_off
	, case when b.have_active_dbankpro is null then 0 else 1 end as have_active_dbankpro
from cust_level_dtl_b a
left join
	bdtableu.dm_customer_profile_general_new(where=(date_pr="&yyyymmdd.")) b
		on a.cif = b.cif
;quit;


/*============================================================================================*/
/*=====================================TRUE CASA BALANCE======================================*/
/*============================================================================================*/

options compress=yes;
proc sql;
create table dm_casa_funding_&mmyy. as
select distinct
	b.cif
	, a.flg_funding_cust
	, a.flg_lending_cust
	, a.flg_both_fl_cust
	, b.cod_acct_no
	, b.acct_status_group
	, b.prod_group_level
	, b.balance_idr
	, b.mis_average_balance_idr
	, b.fin_average_balance_idr
from
	cust_level_dtl_c a
inner join
	bdtableu.dm_casa_funding(where=(date_pr="&yyyymmdd." and acct_status_group<>"CLOSED")) b
		on a.cif = b.cif
;quit;

proc sql;
create table fund_acc_smmry_&mmyy. as
select
	cif
	, sum(case when balance_idr<0 and prod_group_level="CA" then balance_idr end) as sum_neg_ca_osbal_idr
	, sum(case when mis_average_balance_idr<0 and prod_group_level="CA" then mis_average_balance_idr end) as sum_mis_neg_avg_ca
	, sum(case when fin_average_balance_idr<0 and prod_group_level="CA" then fin_average_balance_idr end) as sum_fin_neg_avg_ca

	, sum(case when balance_idr<0 and prod_group_level="SA" then balance_idr end) as sum_neg_sa_osbal_idr
	, sum(case when mis_average_balance_idr<0 and prod_group_level="SA" then mis_average_balance_idr end) as sum_mis_neg_avg_sa
	, sum(case when fin_average_balance_idr<0 and prod_group_level="SA" then fin_average_balance_idr end) as sum_fin_neg_avg_sa

	, sum(case when (balance_idr=0 or balance_idr>0) and prod_group_level="CA" then balance_idr end) as sum_pos_ca_osbal_idr
	, sum(case when (mis_average_balance_idr=0 or mis_average_balance_idr>0) and prod_group_level="CA" then mis_average_balance_idr end) as sum_mis_pos_avg_ca
	, sum(case when (fin_average_balance_idr=0 or fin_average_balance_idr>0) and prod_group_level="CA" then fin_average_balance_idr end) as sum_fin_pos_avg_ca

	, sum(case when (balance_idr=0 or balance_idr>0) and prod_group_level="SA" then balance_idr end) as sum_pos_sa_osbal_idr
	, sum(case when (mis_average_balance_idr=0 or mis_average_balance_idr>0) and prod_group_level="SA" then mis_average_balance_idr end) as sum_mis_pos_avg_sa
	, sum(case when (fin_average_balance_idr=0 or fin_average_balance_idr>0) and prod_group_level="SA" then fin_average_balance_idr end) as sum_fin_pos_avg_sa
from
	dm_casa_funding_&mmyy.
group by 1
;quit;

/*old aum definition:*/
/* (a.sum_osbal_td + b.sum_mis_avg_casa + a.sum_aum_off) as sum_aum_on_off*/
/**/
/**/
/*equivalent to new aum definition:*/
/* (sum_osbal_td*/
/*+ sum_mis_pos_avg_ca + sum_mis_neg_avg_ca*/
/*+ sum_mis_pos_avg_sa + sum_mis_pos_avg_sa*/
/*+ sum_aum_off*/
/**/

/*	, (a.sum_osbal_td + b.sum_mis_avg_casa + a.sum_aum_off) as sum_aum_on_off*/

proc sql;
create table cust_level_dtl_d as
select
	a.*
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
from
	cust_level_dtl_c a
left join
	fund_acc_smmry_&mmyy. b
		on a.cif = b.cif
;quit;

/*====================================================================================*/
/*===================================== DCC REG ======================================*/
/*====================================================================================*/

proc sql;
create table cust_level_dtl_e as
select
	a.*
	, case when b.cif is not null then 1 else 0 end as have_active_dcc
from
	cust_level_dtl_d a
left join (
	select distinct input(host_cif_id,12.) as cif
	from impbigdt.bd_dcc_pcc_corp(where=(date_pr="&yyyymmdd."))
	where trim(is_delete) = "N"
) b
	on a.cif = b.cif
;quit;

/***TIME DURATION END TIME***/
%let end=%sysfunc(datetime());
%let duration=%sysfunc(putn(%sysevalf(&end-&start),time.));
%put timecost &duration.;

%mend sme_cust;

%sme_cust(&master_period.);