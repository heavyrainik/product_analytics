  create table fediq_team.{city}_mean_salary_active_{n}_201909 as
    (
        SELECT week, sum(salary) / count(DISTINCT driver_id) as mean_salary
        FROM (SELECT date_trunc('w', dttm) as week,
                     driver_id,
                     count(order_id)       as amount_of_orders,
                     sum(order_cost)       as salary
              FROM fediq_team.{city}_orders_201909
              GROUP BY driver_id, date_trunc('w', dttm)) as A
        WHERE salary IS NOT NULL
            and driver_id IS NOT NULL
            and amount_of_orders > {n-1}
        GROUP BY week
    );
