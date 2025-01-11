%macro smel_bal_per_cif(period);
%let mmyy = %sysfunc(intnx(month,&period.,0,e),mmyyn4.); %put mmyy:&mmyy.;
%let yyyymm = %sysfunc(intnx(month,&period.,0,e),yymmn6.); %put yyyymm:&yyyymm.;

proc sql;
create table avbal_osbal_&mmyy._per_cif as
select
	cif
	, sum(case when acc_type_a in ("loan_active") then balance_idr end) as total_osbal format comma25.2
	, sum(avbal_loan_idr) as total_avbal format comma25.2
from dm_ba_7.sme_acc_dtl_monthly_v2
where period = &yyyymm.
group by 1
;quit;

%mend smel_bal_per_cif;

%smel_bal_per_cif("31jan2023"d);
%smel_bal_per_cif("28feb2023"d);