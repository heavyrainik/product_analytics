create table fediq_team.novoshakhtinsk_dau_201909 as(
with p as (select passenger_id, date_trunc('day', dttm) as day from fediq_team.novochahtinsk_orders_201909 where status='complete')
select count(distinct passenger_id) as DAU, day
from p
group by(day));
