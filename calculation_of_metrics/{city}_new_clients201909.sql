create table fediq_team.{city}_new_clients201909 as (
    select cohort_week, count(passenger_id)
    from (
             select date_trunc('week', min(dttm)) as cohort_week,
                    passenger_id
             from fediq_team.{city}_orders_201909
             where status = 'complete'
             group by passenger_id
         ) as A
    group by cohort_week
);
