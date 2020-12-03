create table fediq_team.{city}_drivers201909 as (
    select week, count(distinct driver_id)
    from (
             select date_trunc('W', dttm) as week, driver_id
             from fediq_team.{city}_orders_201909
             where status = 'complete'
         ) as A
group by week);
