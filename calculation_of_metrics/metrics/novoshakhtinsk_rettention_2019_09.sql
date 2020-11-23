create table fediq_team.novoshakhtinsk_rettention_2019_09 as(with sum_of_dau as(
with p as (select dau, date_trunc('month', day) as month from fediq_team.Nijnii_tagil_dau_201909)
select sum(dau) as sum_of_dau, month
from p
group by(month))
    select 1.0*sum_of_dau / mau as rettention, fediq_team.novochahtinsk_mau_201909.month as month from sum_of_dau, fediq_team.novochahtinsk_mau_201909
where sum_of_dau.month = fediq_team.novochahtinsk_mau_201909.month)
