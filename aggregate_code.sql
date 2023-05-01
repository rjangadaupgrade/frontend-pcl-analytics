create table dw_risk.secondpcl_cycle_end_new as 

With cte as(
select pcl.*, lir.fico_score,
to_char(convert_timezone('America/Los_Angeles',pcl.create_date)::date,'yyyy-mm') as contract_month, 

case when app.is_direct_mail = 'true' then 'DM'
when app.locked_utm_source = 'credit_karma_lightbox' and mlrpe.status = 'ENROLLED' then 'LIGHTBOX Rewards'
when app.locked_utm_source = 'credit_karma_lightbox' then 'LIGHTBOX'
when upper (case when ras.utm_source is not null and ras.utm_source not in ('decline_referral', 'decline_referral_click') then 'RAS-' || ras.utm_source
     else null end) in ('RAS-PL_DASH_CARD','RAS-SECONDLOAN','RAS-SECONDLOAN_CARD') then 'ExistingPL campaign'
when (app.locked_utm_source like '%_ub%' AND 
app.locked_good_source = 'false' AND
app.is_direct_mail = 'false' AND
	app.locked_api_referrer is null AND
	(app.locked_source_system is null or app.locked_source_system <> 'APP_BY_PHONE') AND
	 app.program = 'PCL' and l.loan_id is null) OR
(app.locked_api_referrer in ('DOT_818', 'MONEVO', 'QUIN_STREET', 'THINK_WALLET','EVEN_FINANCIAL', 'FUNDEDCO')) OR
(app.is_direct_mail = 'false' AND
	app.locked_api_referrer is null AND
	(app.locked_source_system is null or app.locked_source_system <> 'APP_BY_PHONE') AND
	(app.locked_utm_source is null or app.locked_utm_source = '') AND
	 app.locked_good_source = 'false' AND
	 app.program = 'PCL' and l.loan_id is null) OR  	 
(app.locked_api_referrer like '%PLFLOW%' OR app.locked_api_referrer = 'ACORN_FINANCE_PFLOW_CARD') then 'High Risk Channel'
when upper (case when ras.utm_source is not null and ras.utm_source not in ('decline_referral', 'decline_referral_click') then 'RAS-' || ras.utm_source
     else null end) in ('RAS-CREDIT_KARMA_CARD') then 'RAS-CREDIT_KARMA_CARD'
when upper (case when ras.utm_source is not null and ras.utm_source not in ('decline_referral', 'decline_referral_click') then 'RAS-' || ras.utm_source
     else null end) in ('RAS-EXPERIAN') then 'RAS-EXPERIAN'
when upper (case when ras.utm_source is not null and ras.utm_source not in ('decline_referral', 'decline_referral_click') then 'RAS-' || ras.utm_source
     else null end) in ('RAS-DIRECTORY_CARD') then 'RAS-DIRECTORY_CARD'
when lap.initiator = 'UPGRADE' then 'XSell'
else 'Others' end as pcl_channel

from dw_risk.firstpcl as pcl
left join loanreview.loan_in_review as lir
     on pcl.id = lir.id
left join decisioning.application as app 
       on app.loan_app_id = lir.id
       and lir.loan_status='OPENED' and lir.program='PCL'
left join lineservicing.master_line_reward_program_enrollment as mlrpe
       on lir.id = mlrpe.master_line_upgrade_account_id
              and mlrpe.status = 'ENROLLED'
left join funnel.loan_application as lap 
       on lap.id = pcl.id
left join (select row_number() over (partition by asset_id order by update_date asc) as rank, asset_id, sub_id, create_date, utm_source, utm_medium, url, event, cookie_id
           from referrer.referrer_activity_audit where event='app_create') as ras 
       on lap.id = ras.asset_id and ras.rank = 1
left join decisioning.offer offer
       on app.selected_offer_id = offer.id
 left join decisioning.credit_decision cd 
       on offer.credit_decision_id = cd.id
 left join invmgt.loan l
       on cd.prior_loan_id = l.loan_id and l.loan_status in ('ISSUED','REISSUED'))

, post_firstpcl_products_count as (
select ssn_hash, id, create_date, contract_month, product_type, fico_score, pcl_channel, cnt_tot_post_accts1_orig, cnt_pcl_post_accts1_orig, cnt_pl_post_accts1_orig, cnt_dep_post_accts1_orig, cnt_hm_post_accts1_orig, cnt_tot_post_accts1_3month, cnt_pcl_post_accts1_3month
, cnt_pl_post_accts1_3month, cnt_dep_post_accts1_3month, cnt_hm_post_accts1_3month, cnt_tot_post_accts1_6month, cnt_pcl_post_accts1_6month, cnt_pl_post_accts1_6month, cnt_dep_post_accts1_6month
, cnt_hm_post_accts1_6month, cnt_tot_post_accts1_12month, cnt_pcl_post_accts1_12month, cnt_pl_post_accts1_12month, cnt_dep_post_accts1_12month, cnt_hm_post_accts1_12month, cnt_tot_post_accts1_today	
, cnt_pcl_post_accts1_today, cnt_pl_post_accts1_today, cnt_dep_post_accts1_today, cnt_hm_post_accts1_today
, (case when cnt_tot_post_accts1_orig > 0 then 1 else 0 end) as  orig_flag
, (case when cnt_tot_post_accts1_3month > 1 then 1 else 0 end) as  post3m_flag
, (case when cnt_tot_post_accts1_6month > 1 then 1 else 0 end) as  post6m_flag
, (case when cnt_tot_post_accts1_12month > 1 then 1 else 0 end) as  post12m_flag
, (case when cnt_tot_post_accts1_today > 1 then 1 else 0 end) as  today_flag
from cte
order by ssn_hash, contract_month)


, prev_products_purchase_details as (
select b.ssn_hash, lir.id, lir.create_date, prod.contract_month, lir.product_type,  prod.pcl_channel as channel,
LEAD(lir.product_type,1) over(partition by b.ssn_hash order by lir.create_date desc) as prev_prod1, 
LEAD(lir.create_date,1) over(partition by b.ssn_hash order by lir.create_date desc) as prev_prod1_date, 
DATEDIFF(day, LEAD(lir.create_date,1) over(partition by b.ssn_hash order by lir.create_date desc), lir.create_date) as days_prod1,
LEAD(prod.pcl_channel,1) over(partition by b.ssn_hash order by lir.create_date desc) as channel_prev_prod1,
 
LEAD(lir.product_type,2) over(partition by b.ssn_hash order by lir.create_date desc) as prev_prod2,
LEAD(lir.create_date,2) over(partition by b.ssn_hash order by lir.create_date desc) as prev_prod2_date,
DATEDIFF(day, LEAD(lir.create_date,2) over(partition by b.ssn_hash order by lir.create_date desc), lir.create_date) as days_prod2,
LEAD(prod.pcl_channel,2) over(partition by b.ssn_hash order by lir.create_date desc) as channel_prev_prod2, 

LEAD(lir.product_type,3) over(partition by b.ssn_hash order by lir.create_date desc) as prev_prod3,
LEAD(lir.create_date,3) over(partition by b.ssn_hash order by lir.create_date desc) as prev_prod3_date,
DATEDIFF(day, LEAD(lir.create_date,3) over(partition by b.ssn_hash order by lir.create_date desc), lir.create_date) as days_prod3,
LEAD(prod.pcl_channel,3) over(partition by b.ssn_hash order by lir.create_date desc) as channel_prev_prod3, 

LEAD(lir.product_type,4) over(partition by b.ssn_hash order by lir.create_date desc) as prev_prod4,
LEAD(lir.create_date,4) over(partition by b.ssn_hash order by lir.create_date desc) as prev_prod4_date,
DATEDIFF(day, LEAD(lir.create_date,4) over(partition by b.ssn_hash order by lir.create_date desc), lir.create_date) as days_prod4,
LEAD(prod.pcl_channel,4) over(partition by b.ssn_hash order by lir.create_date desc) as channel_prev_prod4, 

LEAD(lir.product_type,5) over(partition by b.ssn_hash order by lir.create_date desc) as prev_prod5,
LEAD(lir.create_date,5) over(partition by b.ssn_hash order by lir.create_date desc) as prev_prod5_date,
DATEDIFF(day, LEAD(lir.create_date,5) over(partition by b.ssn_hash order by lir.create_date desc), lir.create_date) as days_prod5,
LEAD(prod.pcl_channel,5) over(partition by b.ssn_hash order by lir.create_date desc) as channel_prev_prod5, 

case when prev_prod1 = 'PERSONAL_CREDIT_LINE' then 1
when prev_prod2 = 'PERSONAL_CREDIT_LINE' then 1
when prev_prod3 = 'PERSONAL_CREDIT_LINE' then 1
when prev_prod4 = 'PERSONAL_CREDIT_LINE' then 1
when prev_prod5 = 'PERSONAL_CREDIT_LINE' then 1
else 0 end as pcl_flag

from loanreview.loan_in_review as lir 
left join funnel.loan_application as lap
on lir.id = lap.id
left join funnel.borrower as b
on lap.borrower_id = b.id
left join post_firstpcl_products_count as prod
on lir.id = prod.id
where lir.loan_status in('OPENED','ISSUED') ---and b.ssn_hash in ('$2a$06$ODTOPzH/PCTTSCbQOSPDO...7IjSnQJk3n04Ld1uQFQGyqwGxHE.y', '$2a$06$ODTOPzH/PCTTSCbQOSPDO.dFvvnG8QsLm7drjljxI8qBMr5xdf2HO')
order by b.ssn_hash, lir.create_date)

, prev_pcl_purchase_details as(
select *,
case when prev_prod1 = 'PERSONAL_CREDIT_LINE' then 'prod1 - PERSONAL_CREDIT_LINE'
when prev_prod2 = 'PERSONAL_CREDIT_LINE' then 'prod2 - PERSONAL_CREDIT_LINE'
when prev_prod3 = 'PERSONAL_CREDIT_LINE' then 'prod3 - PERSONAL_CREDIT_LINE'
when prev_prod4 = 'PERSONAL_CREDIT_LINE' then 'prod4 - PERSONAL_CREDIT_LINE'
when prev_prod5 = 'PERSONAL_CREDIT_LINE' then 'prod5 - PERSONAL_CREDIT_LINE'
else null end as previous_product,

case when prev_prod1 = 'PERSONAL_CREDIT_LINE' then days_prod1
when prev_prod2 = 'PERSONAL_CREDIT_LINE' then days_prod2
when prev_prod3 = 'PERSONAL_CREDIT_LINE' then days_prod3
when prev_prod4 = 'PERSONAL_CREDIT_LINE' then days_prod4
when prev_prod5 = 'PERSONAL_CREDIT_LINE' then days_prod5
else null end as days_diff_twopcl,

case when prev_prod1 = 'PERSONAL_CREDIT_LINE' then channel_prev_prod1
when prev_prod2 = 'PERSONAL_CREDIT_LINE' then channel_prev_prod2
when prev_prod3 = 'PERSONAL_CREDIT_LINE' then channel_prev_prod3
when prev_prod4 = 'PERSONAL_CREDIT_LINE' then channel_prev_prod4
when prev_prod5 = 'PERSONAL_CREDIT_LINE' then channel_prev_prod5
else null end as prev_channel

from prev_products_purchase_details
where product_type = 'PERSONAL_CREDIT_LINE' and pcl_flag = 1)

, all_existing_products as(
select ssn_hash, id, create_date, product_type, channel, previous_product, prev_channel, days_diff_twopcl
from prev_pcl_purchase_details)

, masterline_front_end_pcl as (select 
--mob.*
--,dlp.twopl_6pmt_cap
--,dlp.twopl_6pmt_cap_flag
--dlp.pl_policy_segment
cyc.*
--,dlp.secondpcl_cap
--,dlp.secondpcl_cap_flag
--,dlp.secondloan_cap
--,dlp.secondloan_cap_flag
--,dlp.priorpcl_reward_program_code
--,dlp.decision_application_reward_code
--,dlp.rewards

--,cyc.confirmed_blacklist
,vr1_pcl_post_submit.vr1_pcl_score as vr1_pcl_score_post_submit
,case when vr1_pcl_score is null then 'w:Null'
       when vr1_pcl_score >= 0 and vr1_pcl_score < 0.03 then 'a:[0%-3%)'
        when vr1_pcl_score >= 0.03 and vr1_pcl_score < 0.05 then 'b:[3%-5%)'
        when vr1_pcl_score >= 0.05 and vr1_pcl_score < 0.07 then 'c:[5%-7%)'
        when vr1_pcl_score >= 0.07 and vr1_pcl_score < 0.08 then 'd:[7%-8%)'
        when vr1_pcl_score >= 0.08 and vr1_pcl_score < 0.09 then 'd:[8%-9%)'
        when vr1_pcl_score >= 0.09 and vr1_pcl_score < 0.10 then 'e:[9%-10%)'
        when vr1_pcl_score >= 0.10 and vr1_pcl_score < 0.13 then 'f:[10%-13%)'
        when vr1_pcl_score >= 0.13 and vr1_pcl_score < 0.15 then 'g:[13%-15%)'
        when vr1_pcl_score >= 0.15 and vr1_pcl_score < 0.17 then 'h:[15%-17%)'
        when vr1_pcl_score >= 0.17 and vr1_pcl_score < 0.2 then 'i:[17%-20%)'
        when vr1_pcl_score >= 0.2 and vr1_pcl_score < 0.23 then 'j:[20%-23%)'
        when vr1_pcl_score >= 0.23 and vr1_pcl_score < 0.27 then 'k:[23%-27%)'
        when vr1_pcl_score >= 0.27  then 't:[27%+)'
        end as vr1_band_pcl_post_submit
,case when mob.vr1_score is null then 'w:Null'
       when mob.vr1_score >= 0 and mob.vr1_score < 0.03 then 'a:[0%-3%)'
        when mob.vr1_score >= 0.03 and mob.vr1_score < 0.05 then 'b:[3%-5%)'
        when mob.vr1_score >= 0.05 and mob.vr1_score < 0.07 then 'c:[5%-7%)'
        when mob.vr1_score >= 0.07 and mob.vr1_score < 0.09 then 'd:[7%-9%)'
        when mob.vr1_score >= 0.09 and mob.vr1_score < 0.11 then 'e:[9%-11%)'
        when mob.vr1_score >= 0.11 and mob.vr1_score < 0.13 then 'f:[11%-13%)'
        when mob.vr1_score >= 0.13 and mob.vr1_score < 0.15 then 'g:[13%-15%)'
        when mob.vr1_score >= 0.15 and mob.vr1_score < 0.17 then 'h:[15%-17%)'
        when mob.vr1_score >= 0.17 and mob.vr1_score < 0.2 then 'i:[17%-20%)'
        when mob.vr1_score >= 0.2 and mob.vr1_score < 0.23 then 'j:[20%-23%)'
        when mob.vr1_score >= 0.23 and mob.vr1_score < 0.27 then 'k:[23%-27%)'
        when mob.vr1_score >= 0.27  then 't:[27%+)'
        end as vr1_band
        
,dp.blacklist as blacklist_dpl

,case when cyc.stated_annual_income_cap400k is null or cyc.stated_annual_income_cap400k<0 then 'NA'
when cyc.stated_annual_income_cap400k<50000 then '<50K'
when cyc.stated_annual_income_cap400k>=50000 and cyc.stated_annual_income_cap400k<60000 then '50K-60K'
when cyc.stated_annual_income_cap400k>=60000 and cyc.stated_annual_income_cap400k<70000 then '60K-70K'
when cyc.stated_annual_income_cap400k>=70000 and cyc.stated_annual_income_cap400k<80000 then '70K-80K'
when cyc.stated_annual_income_cap400k>=80000 and cyc.stated_annual_income_cap400k<100000 then '80K-100K'
when cyc.stated_annual_income_cap400k>=100000 and cyc.stated_annual_income_cap400k<125000 then '100K-125K'
when cyc.stated_annual_income_cap400k>125000 then '125K+' else 'NA' end as stated_annual_income_cap400k_band1

,case when cyc.fcf_stated is null or cyc.fcf_stated<0 then 'NA'
when cyc.fcf_stated<2000 then '<2000'
when cyc.fcf_stated>=2000 and cyc.fcf_stated<4000 then '2K-4K'
when cyc.fcf_stated>=4000 and cyc.fcf_stated<6000 then '4K-6K'
when cyc.fcf_stated>=6000 and cyc.fcf_stated<8000 then '6K-8K'
when cyc.fcf_stated>=8000 and cyc.fcf_stated<12000 then '8K-12K'
when cyc.fcf_stated>=12000 and cyc.fcf_stated<15000 then '12K-15K'
when cyc.fcf_stated>=15000 and cyc.fcf_stated<20000 then '15K-20K'
when cyc.fcf_stated>=20000 and cyc.fcf_stated<25000 then '20K-25K'
when cyc.fcf_stated>=25000 and cyc.fcf_stated<30000 then '25K-30K'
when cyc.fcf_stated>=30000 then '30K+' else 'NA' end as fcf_stated_band1

,case when cyc.fcf_verified is null or cyc.fcf_verified<0 then 'NA'
when cyc.fcf_verified<2000 then '<2000'
when cyc.fcf_verified>=2000 and cyc.fcf_verified<4000 then '2K-4K'
when cyc.fcf_verified>=4000 and cyc.fcf_verified<6000 then '4K-6K'
when cyc.fcf_verified>=6000 and cyc.fcf_verified<8000 then '6K-8K'
when cyc.fcf_verified>=8000 and cyc.fcf_verified<12000 then '8K-12K'
when cyc.fcf_verified>=12000 and cyc.fcf_verified<15000 then '12K-15K'
when cyc.fcf_verified>=15000 and cyc.fcf_verified<20000 then '15K-20K'
when cyc.fcf_verified>=20000 and cyc.fcf_verified<25000 then '20K-25K'
when cyc.fcf_verified>=25000 and cyc.fcf_verified<30000 then '25K-30K'
when cyc.fcf_verified>=30000 then '30K+' else 'NA' end as fcf_verified_band1

,case when ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) <= 30 then '<=30'
when ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) > 30 and ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) <= 60 then '30-60'
when ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) > 60 and ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) <= 90 then '60-90'
when ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) > 90 and ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) <= 120 then '90-120'
when ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) > 120 and ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) <= 150 then '120-150'
when ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) > 150 and ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) <= 180 then '150-180'
when ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) > 180 and ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) <= 270 then '180-270'
when ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) > 270 and ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) <= 365 then '270-365'
when ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) >= 365 and ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) <= 545 then '365-545'
when ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) >= 545 and ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) <= 730 then '545-730'
when ((convert_timezone('America/Los_Angeles',cyc.contract_date)::date) - pabi.priorpcl_contractdate) >= 730 then '730+'
else 'NA' end as diff_between_pcl

,case when cyc.priorpcl_reward_program_code=cyc.masterline_enroll_reward_code then 1 else 0 end as same_reward_secondpcl_masterline_enrll
,case when cyc.priorpcl_reward_program_code=cyc.funnel_locked_reward_code then 1 else 0 end as same_reward_secondpcl_funnel_locked

---,case when pabi.paidoff=1 or priorloan3.good3pmt_count=3 then 1 else 0 end as existingpl_ontime3pay

--- Trade line related information
,tu_td.rev_balance
,tu_td.rev_credit_limit
,tu_td.SC_cnt_orig
,tu_td.BC_cnt_orig
,tu_td.SC_bal_orig
,tu_td.BC_bal_orig
,tu_td.SC_CL_orig
,tu_td.BC_CL_orig
,tu_td.SC_orig
,case when tu_td.SC_orig>0 then 1 else 0 end as SC_orig_fl
,tu_td.SC_orig_balance
,tu_td.SC_orig_credit
,tu_td.auth_user_orig
,tu_td.auth_user_orig_balance
,tu_td.auth_user_orig_credit

--- BT user 
,bt.bt_amount
,case when bt.bt_amount>0 then 1 else 0 end as bt_flag

,case when bt.bt_amount>0 then 'BT'
when cyc.draw_cnt is null or cyc.draw_cnt<0 then 'NA'
when cyc.draw_cnt=0 then 'No draw'
when cyc.card_cnt/cyc.draw_cnt >=0.50 then 'Card Draw'
when cyc.card_cnt/cyc.draw_cnt <0.50 then 'ACH Draw'
else 'NA' end as draw_txn_flag, --- Draw transaction type flag

--- Deposit product user
upg_chk.cnt_deposit_products,
upg_chk.deposit_opened,
upg_chk.premier_savings_flag,
upg_chk.premier_savings_na,
upg_chk.savings_flag

,upg_chk.first_deposit_open_date
,case when upg_chk.first_deposit_open_date<mob.contract_date then 1 else 0 end as deposit_before_pcl_flag

,autopay.ap_enrl_cancel_first_payment
,autopay.ap_enrl_cancel_60_days
,autopay.ap_enrl_cancel_30_days
,autopay.ap_enrl_cancel_90_days
,autopay.ap_enrl_cancel_120_days
,autopay.ap_enrl_cancel_180_days


from dw_risk.masterline_cycle_mob_front_end_data mob
left join
(select lir.id as masterline_id
,count(distinct deposit.deposit_account_id) as cnt_deposit_products,
max(case when deposit.deposit_account_id is not null then 1 else 0 end) as deposit_opened,
max(case when deposit.deposit_account_id is not null and deposit_program_definition_code in ('DEP_SV_01','DEP_SV_02','DEP_SV_05') then 1 else 0 end) as premier_savings_flag,
max(case when deposit.deposit_account_id is not null and deposit_program_definition_code not in ('DEP_SV_01','DEP_SV_02','DEP_SV_05') then 1 else 0 end) as premier_savings_na,
max(case when deposit.deposit_account_id is not null and deposit_account_type in ('SAVINGS') then 1 else 0 end) as savings_flag,
min(deposit_open_date) as first_deposit_open_date,
max(deposit_open_date) as last_deposit_open_date
from loanreview.loan_in_review as lir
left join funnel.loan_application as lap
       on lir.id = lap.id
left join funnel.borrower as b
       on lap.borrower_id = b.id
left join (select lir.id as deposit_account_id, ssn_hash,lir.create_date as deposit_open_date
           ,dap.deposit_account_type
           ,lir.program_definition_code as deposit_program_definition_code   
           from loanreview.loan_in_review as lir
           left join funnel.loan_application as lap
                  on lir.id = lap.id
           left join funnel.borrower as b 
                  on lap.borrower_id = b.id
           left join core.deposit_application dap
           on dap.deposit_account_id=lir.id
           where lir.loan_status='OPENED' and lir.product_type='DEPOSIT') as deposit
       on b.ssn_hash = deposit.ssn_hash
where lir.loan_status='OPENED' and lir.program='PCL'
group by 1
)upg_chk
on mob.masterline_id=upg_chk.masterline_id
left join 
(select ml.account_number as masterline_id,count(*) as cnt_draws,sum(draw_amount) as bt_amount
from lineservicing.credit_master_line ml 
join disbursement.draw_request bt on ml.ACCOUNT_NUMBER = bt.MASTER_LINE_NUMBER and bt.DRAW_SOURCE  in  ('BALANCE_TRANSFER')
join lineservicing.draw dr on bt.external_uuid = dr.uuid::text and dr.DRAW_STATUS  ='ONBOARDED'
group by 1)bt
on mob.masterline_id=bt.masterline_id
left join (select * from dw_risk.masterline_cycle_mob where mob=1)cyc
on cyc.masterline_id=mob.masterline_id
left join dw_risk.vr1_pcl_score_submitted vr1_pcl_post_submit
on vr1_pcl_post_submit.loan_id=mob.masterline_id
left join (select masterline_id, MAX(days_past_due) as days_past_due, sum(line_amount) as sublinetotal,MAX(case when is_blacklisted = 'true' then 1 else 0 end) as blacklist,
           --max(investor_id) as investor_id,
           sum(principal_balance) as pbal_total, sum(principal_balance+coalesce(principal_at_charge_off,0)) as pbal_total_inc_chargeoff,
           sum(line_amount)/sum(line_draw_count) as avg_draw_amt, sum(line_amount)/count(subline_id) as avg_subline_amt,max(charge_off_date) as charge_off_date --charge off date is not null
           from core.loan_position_snapshot_subline
           where processing_date = current_date-2
           and term_in_months is not null
           group by 1) as dp
on mob.masterline_id=dp.masterline_id
left join (select pcl.id as masterline_id,
       SUM(case when pabi.program='PCL' and pabi.servicing_account_status='OPEN' and pabi.status in ('Opened','OPENED','Ready to open','READY_TO_OPEN') then 1 else 0 end) as open_prior_PCL,
       SUM(case when pabi.program in ('CORE','CREDIT_KARMA') and pabi.servicing_account_status='OPEN' and pabi.status in ('Issued','ISSUED','Reissued','REISSUED')
           then 1 else 0 end) as open_prior_PL,
       SUM(case when pabi.program in ('CORE','CREDIT_KARMA') and pabi.servicing_account_status='OPEN' then pabi.principal_balance else 0 end) as pl_pbal,
       MAX(case when pabi.program in ('CORE','CREDIT_KARMA') and pabi.servicing_account_status='CLOSED_PAIDOFF' then 1 else 0 end) as paidoff,
       MAX(case when (pabi.program in ('CORE','CREDIT_KARMA') and pabi.status in ('Issued','ISSUED','Reissued','REISSUED') and pabi.servicing_account_status<>'CLOSED_CHARGEOFF') OR
           (pabi.program in ('CORE','CREDIT_KARMA') and pabi.servicing_account_status='CLOSED_PAIDOFF')
           then 1 else 0 end) as existingpl,
       MAX(case when pabi.program='PCL' and pabi.servicing_account_status is not null then pabi.contract_date else null end) as priorpcl_contractdate,
       MAX(case when pabi.program='PCL' and pabi.reward_program_codes is not null then
           LEFT(trim(replace(reward_program_codes, '"', ''),'[|]')+',', CHARINDEX(',',trim(replace(reward_program_codes, '"', ''),'[|]')+',')-1) else null end) as priorpcl_reward_program_code
from loanreview.loan_in_review pcl
left join decisioning.application app on app.loan_app_id = pcl.id
left join decisioning.offer offer on app.selected_offer_id = offer.id
left join decisioning.credit_decision cd on offer.credit_decision_id = cd.id
left join decisioning.prior_app_billing_info pabi on cd.primary_applicant_id = pabi.applicant_id
where pcl.loan_status='OPENED' and pcl.program='PCL' --and substring(app.decisioning_engine_version,2,2)::int>=45
group by 1) as pabi
       on mob.masterline_id = pabi.masterline_id
left join (with priorloan2 as (
             with priorloan as (
                select pcl.id as masterline_id, pabi.prior_app_lir_id, pap.*
                ,case when (within_tolerance_effective_date::date - due_date::date) <= 15 then 1 else 0 end as goodpmt
                ,rank() over (partition by prior_app_billing_info_id order by due_date desc) as bills
                from loanreview.loan_in_review pcl
                left join decisioning.application app on app.loan_app_id = pcl.id
                left join decisioning.offer offer on app.selected_offer_id = offer.id
                left join decisioning.credit_decision cd  on offer.credit_decision_id = cd.id
                left join decisioning.prior_app_billing_info pabi on cd.primary_applicant_id = pabi.applicant_id 
                left join decisioning.prior_app_payment pap on pabi.id =  pap.prior_app_billing_info_id
                where pabi.program in ('CORE','CREDIT_KARMA') and pabi.servicing_account_status='OPEN'
                and pcl.loan_status='OPENED' and pcl.program='PCL' --and substring(app.decisioning_engine_version,2,2)::int>=45
                )
                select masterline_id, prior_app_lir_id, SUM(case when goodpmt = 1 then 1 else 0 end) as good3pmt_count
                from priorloan
                where bills <= 3
                group by 1,2)
                select masterline_id, MIN(good3pmt_count) as good3pmt_count
                from priorloan2
                group by 1) as priorloan3
on mob.masterline_id = priorloan3.masterline_id
left join 
(select credit_decision_id, /*credit_report_id*/ masterline_id
,sum(balance_rev) as rev_balance
,sum(SC_cnt_orig) as rev_credit_limit
,sum(SC_cnt_orig) as SC_cnt_orig
,sum(BC_cnt_orig) as BC_cnt_orig
,sum(SC_bal_orig) as SC_bal_orig
,sum(BC_bal_orig) as BC_bal_orig
,sum(SC_CL_orig) as SC_CL_orig
,sum(BC_CL_orig) as BC_CL_orig
,sum(SC_orig) as SC_orig
,sum(SC_orig_balance) as SC_orig_balance
,sum(SC_orig_credit) as SC_orig_credit
,sum(auth_user_orig) as auth_user_orig
,sum(auth_user_orig_balance) as auth_user_orig_balance
,sum(auth_user_orig_credit) as auth_user_orig_credit

from (select cd.id as credit_decision_id, /*cd.primary_cr_id as credit_report_id,*/ pcl.masterline_id,
case when portfolio_type = 'revolving' and account in ('CC','FX') and date_closed is null then current_balance else 0 end as balance_rev
,case when portfolio_type = 'revolving' and account in ('CC','FX') and date_closed is null then credit_limit else 0 end as CL_rev
,case when portfolio_type = 'revolving' and account in ('SC') and date_closed is null then 1 else 0 end as SC_cnt_orig
,case when portfolio_type = 'revolving' and account in ('BC') and date_closed is null then 1 else 0 end as BC_cnt_orig
,case when portfolio_type = 'revolving' and account in ('SC') and date_closed is null then current_balance else 0 end as SC_bal_orig
,case when portfolio_type = 'revolving' and account in ('BC') and date_closed is null then current_balance else 0 end as BC_bal_orig
,case when portfolio_type = 'revolving' and account in ('SC') and date_closed is null then credit_limit else 0 end as SC_CL_orig
,case when portfolio_type = 'revolving' and account in ('BC') and date_closed is null then credit_limit else 0 end as BC_CL_orig
,case when portfolio_type = 'revolving' and account in ('SC') and date_closed is null then 1 else 0 end as SC_orig
,case when portfolio_type = 'revolving' and account in ('SC') and date_closed is null then current_balance else 0 end as SC_orig_balance
,case when portfolio_type = 'revolving' and account in ('SC') and date_closed is null then credit_limit else 0 end as SC_orig_credit
,case when portfolio_type = 'revolving' and ecoa_designator in ('authorizedUser') and date_closed is null then 1 else 0 end as auth_user_orig
,case when portfolio_type = 'revolving' and ecoa_designator in ('authorizedUser') and date_closed is null then current_balance else 0 end as auth_user_orig_balance
,case when portfolio_type = 'revolving' and ecoa_designator in ('authorizedUser') and date_closed is null then credit_limit else 0 end as auth_user_orig_credit

from dw_risk.pcl_masterline_2 pcl
join decisioning.application as app 
  on app.loan_app_id = pcl.masterline_id
join decisioning.offer offer
  on app.selected_offer_id = offer.id
join decisioning.credit_decision cd 
  on offer.credit_decision_id = cd.id
join decisioning.trade_line as tl  --select ecoa_designator,count(*) from decisioning.trade_line group by 1;
  on cd.primary_cr_id = tl.credit_report_id) a
group by 1,2)tu_td
on tu_td.masterline_id=mob.masterline_id
left join 
(select a.*
,case when ap.status='ACTIVE' then 1 else 0 end as ap_enrolled
,ap.status
--,a.contract_date
,convert_timezone('PST',ap.authorization_date::date)::date as autopay_authorization_date
,convert_timezone('PST',ap.cancellation_date::date)::date as autopay_cancellation_date
,convert_timezone('PST',sub.first_payment_due_date::date)::date as first_payment_due_date
,autopay_authorization_date-a.contract_date as days_diff_ap_enrolled
,case when autopay_cancellation_date is not null then 1 else 0 end as ap_cancelled
,autopay_cancellation_date-autopay_authorization_date as days_diff_ap_cancelled
,coalesce(days_diff_ap_enrolled,days_diff_ap_cancelled,0) as days_diff_ap_enrl_cancel
,case when autopay_cancellation_date-a.contract_date<=30 or autopay_authorization_date-a.contract_date<=30 then 1 else 0 end as ap_enrl_cancel_30_days
,case when autopay_cancellation_date-a.contract_date<=60 or autopay_authorization_date-a.contract_date<=60 then 1 else 0 end as ap_enrl_cancel_60_days
,case when autopay_cancellation_date-a.contract_date<=90 or autopay_authorization_date-a.contract_date<=90 then 1 else 0 end as ap_enrl_cancel_90_days
,case when autopay_cancellation_date-a.contract_date<=120 or autopay_authorization_date-a.contract_date<=120 then 1 else 0 end as ap_enrl_cancel_120_days
,case when autopay_cancellation_date-a.contract_date<=180 or autopay_authorization_date-a.contract_date<=180 then 1 else 0 end as ap_enrl_cancel_180_days
,case when autopay_cancellation_date-a.contract_date<=240 or autopay_authorization_date-a.contract_date<=240 then 1 else 0 end as ap_enrl_cancel_240_days
,case when (autopay_cancellation_date<=first_payment_due_date+5) or autopay_authorization_date>=first_payment_due_date/*(autopay_authorization_date-first_payment_due_date between -5 and 5)*/ or status is null then 0 else 1 end as ap_enrl_cancel_first_payment
,case when a.autopay_orig='true' then 1 else 0 end as autopay_origination
,case when first_payment_due_date is null then 1 else 0 end as no_activity

from dw_risk.pcl_masterline_2 a
left join lineservicing.auto_pay ap
on ap.master_line_account_number=a.masterline_id
left join
(select masterline_id, min(expected_first_month_payment) as first_payment_due_date
from core.loan_position_snapshot_subline
group by 1) as sub
on sub.masterline_id=a.masterline_id
)autopay
on autopay.masterline_id=mob.masterline_id)

select top 1 all_prod.ssn_hash, all_prod.id, all_prod.create_date, all_prod.product_type, all_prod.channel as pcl_channel, all_prod.previous_product, all_prod.prev_channel, all_prod.days_diff_twopcl, pcl.*
from all_existing_products as all_prod
left join masterline_front_end_pcl as pcl
on all_prod.id = pcl.masterline_id

select * from dw_risk.secondpcl_cycle_end_new

---grant select on dw_risk.secondpcl_cycle_end_new to public;