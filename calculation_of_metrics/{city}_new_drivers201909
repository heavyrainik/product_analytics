create table fediq_team.{city}_new_drivers201909 as (
    select cohort_week, count(driver_id)
    from (
             select date_trunc('week', min(dttm)) as cohort_week,
                    driver_id
             from fediq_team.{city}_orders_201909
             where status = 'complete'
             group by driver_id
         ) as A
    group by cohort_week
);
