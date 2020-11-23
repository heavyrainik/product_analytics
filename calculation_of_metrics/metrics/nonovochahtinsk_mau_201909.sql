create table fediq_team.novochahtinsk_mau_201909 as(
with p as (select passenger_id, date_trunc('month', dttm) as month from fediq_team.novochahtinsk_orders_201909 where status = 'complete')
select count(distinct passenger_id) as MAU, month
from p
group by(month));
