create table fediq_team.{city}_WAU_201909 as (
    select week, count(distinct passenger_id) as WAU
    from (
             select date_trunc('W', dttm) as week, passenger_id
             from fediq_team.{city}_orders_201909
             where status = 'complete'
         ) as A
group by week);
