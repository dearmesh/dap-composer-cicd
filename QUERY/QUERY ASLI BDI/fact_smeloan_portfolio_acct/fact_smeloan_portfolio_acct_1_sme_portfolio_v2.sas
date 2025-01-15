%let master_period = "31jul2024"d;

/*================================================ SME PORTFOLIO V2 ================================================*/

%macro main_sme_acc(period);

/***TIME DURATION START TIME***/
%let start= %sysfunc(datetime());

options compress=YES;

%let MIS_source = misdaily; %put mis_source:&mis_source.;

%let print_period = %sysfunc(intnx(month,&period.,0,e),date9.); %put print_period:&print_period.;

*Untuk tanggal di nama tabel (all tables);
%let yyyymmdd = %sysfunc(intnx(month,&period.,0,e),yymmddn8.); %put yyyymmdd:&yyyymmdd.;
%let yyyymm = %sysfunc(intnx(month,&period.,0,e),yymmn6.); %put yyyymm:&yyyymm.;
%let date9_1 = %sysfunc(intnx(month,&period.,-1,e),date9.); %put date9_1:&date9_1.; *31JUL2024
%let date9_2 = %sysfunc(intnx(month,&period.,-2,e),date9.); %put date9_2:&date9_2.;
%let date9_3 = %sysfunc(intnx(month,&period.,-3,e),date9.); %put date9_3:&date9_3.;

*Untuk table source BD&ddmmyy.;
%let ddmmyy = %sysfunc(intnx(month,&period.,0,e),ddmmyyn6.); %put ddmmyy:&ddmmyy.;
%let ddmmyy_end = %sysfunc(intnx(month,&period.,0,e),ddmmyyn6.); %put ddmmyy_end:&ddmmyy_end.;

%let monyy = %sysfunc(intnx(month,&period.,0,e),monyy6.); %put monyy:&monyy.;

%let mmyy = %sysfunc(intnx(month,&period.,0,e),mmyyn4.); %put mmyy:&mmyy.;
%let mmyy_1 = %sysfunc(intnx(month,&period.,-1,e),mmyyn4.); %put mmyy_1:&mmyy_1.;
%let mmyy_2 = %sysfunc(intnx(month,&period.,-2,e),mmyyn4.); %put mmyy_2:&mmyy_2.;
%let mmyy_3 = %sysfunc(intnx(month,&period.,-3,e),mmyyn4.); %put mmyy_3:&mmyy_3.;

%let mmyy_1_str = &mmyy_1.; %put mmyy_1_str:&mmyy_1_str.;
%let mmyy_2_str = &mmyy_2.; %put mmyy_2_str:&mmyy_2_str.;
%let mmyy_3_str = &mmyy_3.; %put mmyy_3_str:&mmyy_3_str.;

%let mmyyyy = %sysfunc(intnx(month,&period.,0,e),mmyyn6.); %put mmyyyy:&mmyyyy.;

/*ALL Funding*/
options compress=YES;
proc sql;
create table funding_a as
select
		a.lob_code
		, input(a.cif_key, 12.) as cif
		, a.account_number
		, a.iso_currency_cd
		, datepart(a.account_open_date) as acc_open_date format date9.
		, datepart(a.account_close_date) as acc_close_date format date9.
		, trim(compress(put(a.account_status,2.))) as fm_ori_acc_status
		, a.balance_idr
		, a.credit_line_id
		, a.product_desc
		, tranwrd(trim(a.product_code_ncbs),".0","") as product_code_ncbs
		, a.line_status
		, d.description as line_stat_desc
		, a.limit_plafon_idr
		, 0 as collectibility	
/*		, b.type as product_type*/
/*		, a.product_typ*/
		, case when b.type is null then a.product_typ else b.type end as prod_grp_type		
		, a.source_data as data_source_a
		, "ffm_casa" as data_source_b
		, a.cur_net_rate as rate format 18.3
/*		, c.MIS_Description as acc_stat_desc*/
		, c.OFSA_Description as account_status
		, a.amt_hld as amount_hold
		, datepart(a.line_start_date) as fm_line_start_dt format date9.
		, datepart(a.maturity_date) as fm_maturity_date format date9.
		, a.ao_business
from (select * from IMPBIGDT.fm_acct_casa where date_pr = "&YYYYMMDD.") a
left join
	dmuworks.dm_len_bcs_sme_prod_mapv2 B on tranwrd(trim(A.product_code_ncbs),".0","") = trim(B.PRODUCT_CODE_NCBS)
left join
	dmuworks.dm_len_sme_acc_stat C on trim(compress(put(a.account_status,2.))) = trim(compress(put(c.MIS_Account_Status,2.)))
left join
	dmuworks.dm_len_sme_line_stat D on trim(compress(a.line_status)) = trim(compress(d.CL_Status))
where a.lob_code in (22,29,31,36,30,27,35)
;quit;

options compress=YES;
proc sql;
create table funding_b as
select distinct
		a.lob_code
		, input(a.cif_key, 12.) as cif
		, a.account_number
		, a.iso_currency_cd
		, datepart(a.account_open_date) as acc_open_date format date9.
		, datepart(a.account_close_date) as acc_close_date format date9.
		, trim(compress(put(a.account_status,2.))) as fm_ori_acc_status
		, balance_idr
		, "" as credit_line_id
		, a.product_desc
		, tranwrd(trim(a.product_code_ncbs),".0","") as product_code_ncbs
		, "" as line_status
		, "" as line_stat_desc
		, 0 as limit_plafon_idr
		, 0 as collectibility	
/*		, b.type as product_type*/
/*		, "TD" as product_typ*/
		, case when b.type is null then a.product_typ else b.type end as prod_grp_type
		, a.source_data as data_source_a
		, "ffm_td" as data_source_b
		, a.cur_net_rate as rate format 18.3
/*		, c.MIS_Description as acc_stat_desc*/
		, c.OFSA_Description as account_status		
		, 0 as amount_hold
		, . as fm_line_start_dt
		, datepart(a.maturity_date) as fm_maturity_date format date9.
		, a.ao_business
from (select * from IMPBIGDT.fm_acct_td where date_pr = "&YYYYMMDD.") a
left join
	dmuworks.dm_len_bcs_sme_prod_mapv2(where=(trim(lower(class))="funding")) B on tranwrd(trim(A.product_code_ncbs),".0","") = trim(B.PRODUCT_CODE_NCBS)
left join
	dmuworks.dm_len_sme_acc_stat C on trim(compress(put(a.account_status,2.))) = trim(compress(put(c.MIS_Account_Status,2.)))
where a.lob_code in (36,22,30,29,27,31,35)
;quit;

/*ALL Lending*/

options compress=YES;
proc sql;
create table lending_a as
select
		a.lob_code
		, input(a.cif_key, 12.) as cif
		, a.account_number
		, a.iso_currency_cd
		, datepart(a.account_open_date) as acc_open_date format date9.
		, datepart(a.account_close_date) as acc_close_date format date9.
		, a.account_status as fm_ori_acc_status
		, case when (a.balance_idr - a.amt_arrears_due) is null
			then a.balance_idr else (a.balance_idr - a.amt_arrears_due) 
			end as balance_idr
		, a.credit_line_id
		, a.product_desc
		, tranwrd(trim(a.product_code_ncbs),".0","") as product_code_ncbs
		, a.line_status
		, d.description as line_stat_desc
		, a.limit_plafon_idr
		, case when substr(put(a.collectibility, 2.), 1, 1) = "5" then 5
		when substr(put(a.collectibility, 2.), 1, 1) = "4" then 4
		when a.collectibility = 10 then 1
		when a.collectibility = 20 then 2
		when a.collectibility = 30 then 3
		else a.collectibility
		end as col
/*		, b.type as product_type*/
/*		, a.product_typ*/
		, case when b.type is null then a.product_typ else b.type end as prod_grp_type
		, a.source_data as data_source_a
		, "lfm_loan" as data_source_b
		, a.cur_net_rate as rate format 18.3
/*		, c.MIS_Description as acc_stat_desc*/
		, c.OFSA_Description as account_status		
		, 0 as amount_hold
		, datepart(a.line_start_date) as fm_line_start_dt format date9.
		, datepart(a.maturity_date) as fm_maturity_date format date9.
		, a.ao_business
from (select * from IMPBIGDT.fm_acct_loan where date_pr = "&YYYYMMDD.") a
left join
	dmuworks.dm_len_bcs_sme_prod_mapv2 B on tranwrd(trim(a.product_code_ncbs),".0","") = trim(B.PRODUCT_CODE_NCBS)
left join
	dmuworks.dm_len_sme_acc_stat C on trim(compress(a.account_status)) = trim(compress(put(c.MIS_Account_Status,2.)))
left join
	dmuworks.dm_len_sme_line_stat D on trim(compress(a.line_status)) = trim(compress(d.CL_Status))
where
	a.line_status in ("A","O", "") and a.lob_code in (22,29,31)
;quit;

options compress=yes;
proc sql;
create table all_acc as
select * from funding_a
union all
select * from funding_b
union all
select * from lending_a
;quit;

options compress=yes;
proc sql;
create table sme_portfolio as
select distinct *, "loan_active" as acc_type_a, "sme_acc" as acc_type_b from all_acc
where
		data_source_b = "lfm_loan"
		and lob_code in (22,29,31)
		and line_status in ("A","O","")
		and not (prod_grp_type in ("TF") and trim(compress(fm_ori_acc_status)) in ("0"))
		and not (prod_grp_type in ("KAB") and (trim(compress(fm_ori_acc_status)) in ("1") or balance_idr = 0))
union all

select distinct *, "fund_loan_active" as acc_type_a, "sme_acc" as acc_type_b from all_acc
where
		data_source_b = "ffm_casa"
		and lob_code in (22,29,31)
		and line_status in ("O")
		and prod_grp_type not in ("SA")
		and trim(compress(fm_ori_acc_status)) not in ("1","5")
union all

select distinct *, "fund_td" as acc_type_a, "sme_acc" as acc_type_b from all_acc
where
		data_source_b = "ffm_td"
		and lob_code in (36,22,30,29,27,31,35)
		and trim(compress(fm_ori_acc_status)) not in ("1","5")
union all

select distinct *, "fund_casa" as acc_type_a, "sme_acc" as acc_type_b from all_acc
where

		/*new condition for testing_v2*/
		(data_source_b="ffm_casa" 
		and lob_code in (22,29,31) 
		and line_status not in ("O") 
		and trim(compress(fm_ori_acc_status)) not in ("1","5")
		)
		or 
		(data_source_b="ffm_casa"
		and lob_code in (36,30,27,35)
		and trim(compress(fm_ori_acc_status)) not in ("1","5")
		)
		or
		(data_source_b="ffm_casa" 
		and lob_code in (22,29,31) 
		and prod_grp_type in ("SA")
		and trim(compress(fm_ori_acc_status)) not in ("1","5")
		)
;quit;

options compress=yes;
/*use COD_ACCT_NO*/
data ch_od_lim_a;
set impbigdt.bd_ch_od_limit(where=(date_pr="&YYYYMMDD."))
;run;

/* ----- RANK -> this method is more like ROWNUM ----- */
proc sort data = ch_od_lim_a ; by cod_acct_no descending DAT_LIMIT_END ;run;

data ch_od_lim;
set ch_od_lim_a;
by cod_acct_no;
retain REFF;
format REFF 20.;
if first.cod_acct_no then REFF=1;
else REFF=REFF+1;
run;
/* ----- RANK -> this method is more like ROWNUM ----- */

proc sql;
create table ch_od_lim_final (drop=REFF) as
select *
from ch_od_lim
where REFF=1;
quit;

options compress=yes;
proc sql;
create table sme_portfolio_&mmyy.(drop = fm_line_start_dt fm_maturity_date) as
select a.*
	, case
		when a.product_code_ncbs IN ('502','517','502.0','517.0','502,0','517,0','POU')
			OR
			a.product_desc LIKE '%SUPPLY CHAIN%' then "Y" else "N" end as acc_flg_difi 
	, case when 
		a.fm_line_start_dt = "1jan1800"d then datepart(c.DAT_LIMIT_START)
	else 
		coalesce(a.fm_line_start_dt, datepart(c.DAT_LIMIT_START))
	end as
		line_start_dt format date9.
	, case when
		a.fm_maturity_date = "1jan1800"d then datepart(c.DAT_LIMIT_END)
	else
		coalesce(a.fm_maturity_date, datepart(c.DAT_LIMIT_END))
	end as
		line_maturity_dt format date9.
from
	sme_portfolio a
left join
	ch_od_lim_final c on trim(a.account_number) = trim(c.COD_ACCT_NO)
order by
	cif, account_number
;quit;

/*proc sql;*/
/*create table sme_portfolio_&mmyy. as*/
/*select distinct*/
/*	a.*	*/
/*	, coalesce(b.bdi_avg_book_bal_idr,0) as fmcasamth_avbalidr*/
/*	, coalesce(c.bdi_avg_book_bal_idr,0) as fmloanmth_avbalidr*/
/*	, coalesce(d.bdi_avg_book_bal_idr,0) as fmtdmth_avbalidr*/
/*from*/
/*	sme_portfolio_&mmyy. a*/
/*left join impbigdt.fm_acct_casa_mth(where=(date_pr="&yyyymmdd.")) b*/
/*	on trim(a.account_number) = trim(b.account_number) and a.data_source_b in ("ffm_casa","lfm_loan")*/
/*left join impbigdt.fm_acct_loan_mth(where=(date_pr="&yyyymmdd.")) c*/
/*	on trim(a.account_number) = trim(c.account_number) and a.data_source_b in ("ffm_casa","lfm_loan")*/
/*left join impbigdt.fm_acct_td_mth(where=(date_pr="&yyyymmdd.")) d*/
/*	on trim(a.account_number) = trim(d.account_number) and a.data_source_b in ("ffm_td")*/
/*;quit;*/

/***TIME DURATION END TIME***/
%let end=%sysfunc(datetime());
%let duration=%sysfunc(putn(%sysevalf(&end-&start),time.));
%put timecost &duration.;

%mend main_sme_acc;

%main_sme_acc(&master_period.);