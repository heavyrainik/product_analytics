create table fediq_team.{city}_dau_201909 as (
    select day, count(distinct passenger_id) as DAU
    from (
             select date_trunc('day', dttm) as day, passenger_id
             from fediq_team.{city}_orders_201909
             where status = 'complete'
         ) as A
group by day);
