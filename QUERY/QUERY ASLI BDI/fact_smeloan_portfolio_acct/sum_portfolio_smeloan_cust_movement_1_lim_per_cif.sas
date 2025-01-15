%macro smel_limit_per_cif(period);
%let mmyy = %sysfunc(intnx(month,&period.,0,e),mmyyn4.); %put mmyy:&mmyy.;
%let yyyymm = %sysfunc(intnx(month,&period.,0,e),yymmn6.); %put yyyymm:&yyyymm.;

proc sql;
create table sme_loan_limit_&mmyy. as
select
	cif
	, credit_line_id
	, case when prod_grp_type not in ("KAB") then "not_KAB" else prod_grp_type end as prod_group
	, case when prod_grp_type in ("KAB") then balance_idr else limit_plafon_idr end as limit_idr
from dm_ba_7.sme_acc_dtl_monthly_v2(where=(period=&yyyymm.))
where acc_type_a in ("fund_loan_active","loan_active")
;quit;

proc sql;
create table kab_loan_lim_per_cif_&mmyy. as
select cif, prod_group, sum(limit_idr) as total_limit
from sme_loan_limit_&mmyy.
where prod_group = "KAB"
group by 1,2
;quit;

proc sql;
create table nonkab_loan_lim_per_cif_&mmyy. as
select cif, prod_group, sum(limit_idr) as total_limit
from (
	select distinct cif, prod_group, credit_line_id, limit_idr
	from sme_loan_limit_&mmyy.
	where prod_group = "not_KAB"
)
group by 1,2
;quit;

proc sql;
create table lim_per_cif_a as
select * from (
	select * from kab_loan_lim_per_cif_&mmyy.
	union all
	select * from nonkab_loan_lim_per_cif_&mmyy.
)
order by cif, prod_group
;quit;

proc sql;
create table final_lim_per_cif_&mmyy. as
select cif, sum(total_limit) as total_limit format comma25.2
from lim_per_cif_a
group by 1
;quit;

%mend smel_limit_per_cif;

%smel_limit_per_cif("31jan2023"d);
%smel_limit_per_cif("28feb2023"d);

/*proc sql;*/
/*create table cek_available_period as*/
/*select*/
/*	period*/
/*	, count(distinct cif) as noc format comma25.*/
/*	, count(distinct account_number) as noa format comma25.*/
/*from dm_ba_7.sme_acc_dtl_monthly_v2*/
/*group by 1*/
/*order by 1*/
/*;quit;*/
/**/
/*proc sql;*/
/*create table cek_available_period_b as*/
/*select*/
/*	period*/
/*	, count(distinct cif) as noc format comma25.*/
/*	, count(distinct account_number) as noa format comma25.*/
/*from dm_ba_7.sme_acc_dtl_monthly*/
/*group by 1*/
/*order by 1*/
/*;quit;*/
/**/
/*proc sql;*/
/*create table compare_v1_v2 as*/
/*select*/
/*	a.period*/
/*	, a.noc as noc_v2*/
/*	, b.noc as noc_v1*/
/*	, a.noa as noa_v2*/
/*	, b.noa as noa_v1*/
/*from cek_available_period a*/
/*left join cek_available_period_b b on a.period = b.period*/
/*;quit;*/