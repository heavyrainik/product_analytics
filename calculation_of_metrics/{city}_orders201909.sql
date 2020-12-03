create table fediq_team.{city}_orders201909 as (
    select week, count(distinct order_id)
    from (
             select date_trunc('W', dttm) as week, order_id
             from fediq_team.{city}_orders_201909
         ) as A
group by week);
