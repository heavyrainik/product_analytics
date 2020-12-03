-- Считаем недельные когорты. Пусть когорта идентифицируется по началу недели.

-- Посчитаем, к какой когорте относится каждый пользователь
-- Для этого найдем дату первого успешного заказа для каждого пользователя.
-- И выковыряем из даты неделю.
create temporary table passenger_to_cohort as
select
    passenger_id,
    date_trunc('week', min(dttm)) as cohort_week
from fediq_team.{city}_orders_201909
where status = 'complete' and
group by passenger_id;

-- Заранее посчитаем размер каждой когорты. Так будет удобнее считать ретеншн.
create temporary table cohort_size as
select
    cohort_week,
    count(passenger_id) as cohort_size
from passenger_to_cohort
group by cohort_week;

-- Строим, собственно, когортную табличку.
create table fediq_team.{city}_retention_cohorts201909 as (
-- Для каждого заказа определим, к какой неделе он относится.
-- Подтянем к заказу информацию о когорте пользователя, а такжо о размере этой когорты
    with order_cohorts as (
        select o.passenger_id,
               date_trunc('week', o.dttm) as current_week,
               p2c.cohort_week,
               s.cohort_size
        from fediq_team.{city}_table_201909 o
                 inner join passenger_to_cohort p2c
                            on o.passenger_id = p2c.passenger_id
                 inner join cohort_size s
                            on p2c.cohort_week = s.cohort_week
        where o.status = 'complete'
    )
-- Строим когортную табличку
    select A.*, (A.unique_users::float/cs.cohort_size) as retention
    from (
             select
                 -- Группируем все заказы по текущей неделе и пользовательской когорте
                 cohort_week,
                 current_week,
                 -- Количество заказов в когорте в эту неделю
                 count(*)                     as orders,
                 -- Число уников в когорте в эту неделю
                 count(distinct passenger_id) as unique_users
             from order_cohorts
             group by cohort_week,
                      current_week
         ) as A
             inner join cohort_size cs
             on a.cohort_week = cs.cohort_week
);
