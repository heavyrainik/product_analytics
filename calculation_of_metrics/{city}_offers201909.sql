create table fediq_team.{city}_offers201909 as (
    select week, count(distinct offer_id)
    from (
             select date_trunc('W', dttm) as week, offer_id
             from fediq_team.{city}_orders_201909
         ) as A
group by week);
