create table fediq_team.{city}_rides201909 as (
    SELECT date_trunc('W', dttm) as week, car_class, count(order_id)
    FROM fediq_team.{city}_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
    GROUP BY date_trunc('W', dttm), car_class
);
