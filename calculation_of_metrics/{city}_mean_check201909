create table fediq_team.{city}_mean_check201909 as (
    SELECT date_trunc('W', dttm)                    as week,
           car_class,
           round(sum(order_cost) / count(order_id)) as mean_check,
           count(order_id)
    FROM fediq_team.{city}_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
    GROUP BY date_trunc('W', dttm), car_class
)
