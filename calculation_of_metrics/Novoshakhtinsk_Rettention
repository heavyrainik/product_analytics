 create table novoshakhtinsk_rettention as(with sum_of_dau as(
with p as (select dau, date_trunc('month', day) as month from novoshakhtinsk_dau)
select sum(dau) as sum_of_dau, month
from p
group by(month))
    select 1.0*sum_of_dau / mau as rettention, novoshakhtinsk_mau.month as month from sum_of_dau, novoshakhtinsk_mau
where sum_of_dau.month = novoshakhtinsk_mau.month)
