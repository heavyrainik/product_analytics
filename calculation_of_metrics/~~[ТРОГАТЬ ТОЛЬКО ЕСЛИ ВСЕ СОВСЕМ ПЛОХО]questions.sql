CREATE TABLE fediq_team.parsed_orders_table_201909 as With orders as (
    select
        doc::json as dj
    from common.order_proc_fediq
),
parsed_orders as (
    select

        (dj ->> '_id')::text                                                          as order_id,
        (dj ->> 'created')::timestamp                                                      as  dttm,
        (dj -> 'order' -> 'performer' ->> 'driver_license')::text                                     as driver_id,
        (dj -> 'order' ->> 'user_id')::text                                           as passenger_id,
        (dj -> 'order' ->> 'taxi_status')::text                                       as status,
        (dj -> 'order' -> 'request' -> 'source' -> 'geopoint' ->> 0)::decimal            as start_point_a_lon,
        (dj -> 'order' -> 'request' -> 'source' -> 'geopoint' ->> 1)::decimal            as start_point_a_lat,
        (dj -> 'order' -> 'request' -> 'destinations' -> 0 -> 'geopoint' ->> 0)::decimal as dest_point_b_lon,
        (dj -> 'order' -> 'request' -> 'destinations' -> 0 -> 'geopoint' ->> 1)::decimal as dest_point_b_lat,
        (dj -> 'order' ->> 'cost')::decimal                                       as order_cost,
        (dj ->'order' ->> 'application')::text                                 as order_interface,
        (dj -> 'order_info' -> 'statistics' -> 'status_updates' -> 5 -> 'p' ->> 'time')::timestamp  as start_order_dttm,
        (dj -> 'order_info' -> 'statistics' -> 'status_updates' -> 6 -> 'p' ->> 'time')::timestamp  as finish_order_dttm,
        (dj -> 'order_info' -> 'statistics' -> 'status_updates' -> 4 -> 'p' ->> 'time')::timestamp  as real_car_arrival_dttm,
        (dj -> 'order' -> 'request' ->> 'offer')::text                  as offer_id,
        (dj->'order' -> 'calc' ->> 'time')::decimal as excepted_time_in_trip,
        (dj -> 'order' -> 'performer' -> 'tariff' ->> 'class')::text as car_class,
    extract(epoch from ((dj -> 'order_info' -> 'statistics' -> 'status_updates' -> 6 -> 'p' ->> 'time')::timestamp - (dj -> 'order_info' -> 'statistics' -> 'status_updates' -> 5 -> 'p' ->> 'time')::timestamp) ) as real_time_in_trip

    from orders
)
select *
from parsed_orders;

create table fediq_team.pins_parsed_table_201909 as
with pin as(
select
    doc::json as dj
from common.pinstats_fediq
),
parsed_pin as (
    select
        (dj ->> '_id') as pin_id,
        (dj ->> 'offer_id') as offer_id,
        (dj ->> 'order_id') as order_id,
        (dj ->> 'user_id') as user_id,
        (dj ->> 'created')::timestamp as created_time,
        (dj ->> 'estimated_waiting') as estimated_waiting

    from pin
)
select *
from parsed_pin;

create table fediq_team.parsed_offers_table_201909 as
with offer as(
select
    doc::json as dj
from common.offers_fediq
),
parsed_offer as (
    select
        (dj ->> '_id') as offer_id,
        (dj ->> 'user_id') as user_id,
        (dj ->> 'created') as created_time,
        (dj ->> 'time') as expected_time_in_trip_offer,
        (dj -> 'price_modifiers' -> 'items' -> 0 ->> 'reason') as ya_plus_subscriber,
        (dj-> 'route' -> 0 ->> 1) as start_point_lat

    from offer
)
select *
from parsed_offer;

create table fediq_team.offers_table_201909 as
(
    SELECT df.offer_id, df.start_ofer_session, df.finish_offer_session, user_id, df.start_point_lat
    FROM (SELECT offer.offer_id, min(p.created_time) as start_ofer_session, max(p.created_time), offer.start_point_lat as finish_offer_session
    from fediq_team.parsed_offers_table_201909 as offer
    RIGHT JOIN fediq_team.pins_parsed_table_201909 as p ON offer.offer_id = p.offer_id
    GROUP BY offer.offer_id, offer.start_point_lat) as df
    INNER JOIN fediq_team.parsed_offers_table_201909 ON fediq_team.parsed_offers_table_201909 .offer_id = df.offer_id
);

create table fediq_team.orders_tables as
(
    SELECT o.*, p.created_time, p.pin_id, p.estimated_waiting
    from fediq_team.parsed_orders_table_201909 as o
    LEFT JOIN fediq_team.pins_parsed_table_201909 as p ON o.order_id = p.order_id
);

create table fediq_team.users_table as
(
    SELECT user_id, start_time
    FROM fediq_team.parsed_offers_table_201909
    JOIN (SELECT fediq_team.parsed_orders_table_201909.passenger_id, min (fediq_team.parsed_orders_table_201909.dttm) as start_time
    FROM fediq_team.parsed_orders_table_201909
    GROUP BY passenger_id) as A ON fediq_team.parsed_offers_table_201909.user_id = A.passenger_id
);

create table fediq_team.novochahtinsk_orders_201909 as(
    SELECT *
    FROM fediq_team.orders_table_201909
    WHERE 47<=start_point_a_lat and start_point_a_lat <= 48
);


create table fediq_team.novochahtinsk_mau_201909 as(
with p as (select passenger_id, date_trunc('month', dttm) as month from fediq_team.novochahtinsk_orders_201909 where status = 'complete')
select count(distinct passenger_id) as MAU, month
from p
group by(month));

create table fediq_team.novoshakhtinsk_dau_201909 as(
with p as (select passenger_id, date_trunc('day', dttm) as day from fediq_team.novochahtinsk_orders_201909 where status='complete')
select count(distinct passenger_id) as DAU, day
from p
group by(day));

create table fediq_team.novoshakhtinsk_rettention_2019_09 as(with sum_of_dau as(
with p as (select dau, date_trunc('month', day) as month from fediq_team.novoshakhtinsk_dau_201909)
select sum(dau) as sum_of_dau, month
from p
group by(month))
    select 1.0*sum_of_dau / mau as rettention, fediq_team.novochahtinsk_mau_201909.month as month from sum_of_dau, fediq_team.novochahtinsk_mau_201909
where sum_of_dau.month = fediq_team.novochahtinsk_mau_201909.month)

create table fediq_team.novochahtinsk_dolya_ottoka_201909 as (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and dttm < '2018-02-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and dttm < '2018-02-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and dttm < '2018-03-01' and dttm >= '2018-02-01 00:00:00'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-02-01')::timestamp as month from j, l);

insert into fediq_team.novochahtinsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-02-01' <= dttm and dttm < '2018-03-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-02-01' <= dttm and dttm < '2018-03-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-03-01' <= dttm and dttm < '2018-04-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-03-01')::timestamp as month from j, l);

insert into fediq_team.novochahtinsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-03-01' <= dttm and dttm < '2018-04-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-03-01' <= dttm and dttm < '2018-04-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-04-01' <= dttm and dttm < '2018-05-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-04-01')::timestamp as month from j, l);

insert into fediq_team.novochahtinsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-04-01' <= dttm and dttm < '2018-05-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-04-01' <= dttm and dttm < '2018-05-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-05-01' <= dttm and dttm < '2018-06-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-05-01')::timestamp as month from j, l);

insert into fediq_team.novochahtinsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-05-01' <= dttm and dttm < '2018-06-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-05-01' <= dttm and dttm < '2018-06-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-06-01' <= dttm and dttm < '2018-07-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-06-01')::timestamp as month from j, l);

insert into fediq_team.novochahtinsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-06-01' <= dttm and dttm < '2018-07-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-06-01' <= dttm and dttm < '2018-07-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-07-01' <= dttm and dttm < '2018-08-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-07-01')::timestamp as month from j, l);

insert into fediq_team.novochahtinsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-07-01' <= dttm and dttm < '2018-08-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-07-01' <= dttm and dttm < '2018-08-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-08-01' <= dttm and dttm < '2018-09-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-08-01')::timestamp as month from j, l);

insert into fediq_team.novochahtinsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-08-01' <= dttm and dttm < '2018-09-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-08-01' <= dttm and dttm < '2018-09-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-09-01' <= dttm and dttm < '2018-10-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-09-01')::timestamp as month from j, l);

insert into fediq_team.novochahtinsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-09-01' <= dttm and dttm < '2018-10-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-09-01' <= dttm and dttm < '2018-10-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-10-01' <= dttm and dttm < '2018-11-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-10-01')::timestamp as month from j, l);

insert into fediq_team.novochahtinsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-10-01' <= dttm and dttm < '2018-11-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-10-01' <= dttm and dttm < '2018-11-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-11-01' <= dttm and dttm < '2018-12-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-11-01')::timestamp as month from j, l);

insert into fediq_team.novochahtinsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-11-01' <= dttm and dttm < '2018-12-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-11-01' <= dttm and dttm < '2018-12-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-12-01' <= dttm and dttm < '2019-01-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-12-01')::timestamp as month from j, l);

insert into fediq_team.novochahtinsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-12-01' <= dttm and dttm < '2019-01-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2018-12-01' <= dttm and dttm < '2019-01-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-01-01' <= dttm and dttm < '2019-02-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-01-01')::timestamp as month from j, l);

insert into fediq_team.novochahtinsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-01-01' <= dttm and dttm < '2019-02-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-01-01' <= dttm and dttm < '2019-02-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-02-01' <= dttm and dttm < '2019-03-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-02-01')::timestamp as month from j, l);

insert into fediq_team.novochahtinsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-02-01' <= dttm and dttm < '2019-03-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-02-01' <= dttm and dttm < '2019-03-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-03-01' <= dttm and dttm < '2019-04-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-03-01')::timestamp as month from j, l);

insert into fediq_team.novochahtinsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-03-01' <= dttm and dttm < '2019-04-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-03-01' <= dttm and dttm < '2019-04-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-04-01' <= dttm and dttm < '2019-05-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-04-01')::timestamp as month from j, l);

insert into fediq_team.novochahtinsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-04-01' <= dttm and dttm < '2019-05-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-04-01' <= dttm and dttm < '2019-05-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-05-01' <= dttm and dttm < '2019-06-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-05-01')::timestamp as month from j, l)

insert into fediq_team.novochahtinsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-05-01' <= dttm and dttm < '2019-06-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-05-01' <= dttm and dttm < '2019-06-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-06-01' <= dttm and dttm < '2019-07-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-06-01')::timestamp as month from j, l)

insert into fediq_team.novochahtinsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-06-01' <= dttm and dttm < '2019-07-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-06-01' <= dttm and dttm < '2019-07-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-07-01' <= dttm and dttm < '2019-08-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-07-01')::timestamp as month from j, l);

insert into fediq_team.novochahtinsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-07-01' <= dttm and dttm < '2019-08-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-07-01' <= dttm and dttm < '2019-08-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-08-01' <= dttm and dttm < '2019-09-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-08-01')::timestamp as month from j, l);

insert into fediq_team.novochahtinsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-08-01' <= dttm and dttm < '2019-09-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-08-01' <= dttm and dttm < '2019-09-01'
except
select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where status = 'complete' and '2019-09-01' <= dttm and dttm < '2019-10-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-09-01')::timestamp as month from j, l);

create table fediq_team.novochahtinsk_rides_count_201909 as (
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_01_01' <= dttm
      and dttm <= '2018_02_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_02_01' <= dttm
      and dttm <= '2018_03_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_03_01' <= dttm
      and dttm <= '2018_04_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_04_01' <= dttm
      and dttm <= '2018_05_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_05_01' <= dttm
      and dttm <= '2018_06_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_06_01' <= dttm
      and dttm <= '2018_07_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_07_01' <= dttm
      and dttm <= '2018_08_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_08_01' <= dttm
      and dttm <= '2018_09_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_09_01' <= dttm
      and dttm <= '2018_10_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_10_01' <= dttm
      and dttm <= '2018_11_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_11_01' <= dttm
      and dttm <= '2018_12_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_12_01' <= dttm
      and dttm <= '2019_01_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_01_01' <= dttm
      and dttm <= '2019_02_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_02_01' <= dttm
      and dttm <= '2019_03_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_03_01' <= dttm
      and dttm <= '2019_04_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_04_01' <= dttm
      and dttm <= '2019_05_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_05_01' <= dttm
      and dttm <= '2019_06_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_06_01' <= dttm
      and dttm <= '2019_07_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_07_01' <= dttm
      and dttm <= '2019_08_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_08_01' <= dttm
      and dttm <= '2019_09_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_09_01' <= dttm
      and dttm <= '2019_10_01'
    GROUP BY car_class
);

create table fediq_team.novochahtinsk_mean_check_201909 as (
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_01_01' <= dttm
      and dttm <= '2018_02_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_02_01' <= dttm
      and dttm <= '2018_03_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_03_01' <= dttm
      and dttm <= '2018_04_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_04_01' <= dttm
      and dttm <= '2018_05_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_05_01' <= dttm
      and dttm <= '2018_06_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_06_01' <= dttm
      and dttm <= '2018_07_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_07_01' <= dttm
      and dttm <= '2018_08_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_08_01' <= dttm
      and dttm <= '2018_09_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_09_01' <= dttm
      and dttm <= '2018_10_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_10_01' <= dttm
      and dttm <= '2018_11_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_11_01' <= dttm
      and dttm <= '2018_12_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_12_01' <= dttm
      and dttm <= '2019_01_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_01_01' <= dttm
      and dttm <= '2019_02_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_02_01' <= dttm
      and dttm <= '2019_03_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_03_01' <= dttm
      and dttm <= '2019_04_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_04_01' <= dttm
      and dttm <= '2019_05_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_05_01' <= dttm
      and dttm <= '2019_06_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_06_01' <= dttm
      and dttm <= '2019_07_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_07_01' <= dttm
      and dttm <= '2019_08_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_08_01' <= dttm
      and dttm <= '2019_09_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_09_01' <= dttm
      and dttm <= '2019_10_01'
    GROUP BY car_class
);

create table fediq_team.unique_offers as(
    SELECT user_id, (max(created_time))::timestamp as dttm, (start_point_lat)::float8
    FROM fediq_team.parsed_offers_table_201909
    GROUP BY user_id, start_point_lat
);

create table fediq_team.novochahtinsk_offers_count_201909 as (
    SELECT CAST('2018_01_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_01_01')::timestamp
      AND (created_time)::timestamp < ('2018_02_01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2018_02_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_02_01')::timestamp
      AND (created_time)::timestamp < ('2018_03_01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2018_03_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_03_01')::timestamp
      AND (created_time)::timestamp < ('2018_04_01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2018_04_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_04_01')::timestamp
      AND (created_time)::timestamp < ('2018_05_01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2018_05_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_05_01')::timestamp
      AND (created_time)::timestamp < ('2018_06_01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2018_06_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_06_01')::timestamp
      AND (created_time)::timestamp < ('2018_07_01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2018_07_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_07_01')::timestamp
      AND (created_time)::timestamp < ('2018_08_01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2018_08_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_08_01')::timestamp
      AND (created_time)::timestamp < ('2018_09_01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2018_09_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_09_01')::timestamp
      AND (created_time)::timestamp < ('2018_10_01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2018_10_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_10_01')::timestamp
      AND (created_time)::timestamp < ('2018_11_01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2018_11_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_11_01')::timestamp
      AND (created_time)::timestamp < ('2018_12_01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2018_12_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_12_01')::timestamp
      AND (created_time)::timestamp < ('2019_01_01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2019_01_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019_01_01')::timestamp
      AND (created_time)::timestamp < ('2019_02_01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2019_02_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019_02_01')::timestamp
      AND (created_time)::timestamp < ('2019_03_01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2019_03_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019_03_01')::timestamp
      AND (created_time)::timestamp < ('2019_04_01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2019_04_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019_04_01')::timestamp
      AND (created_time)::timestamp < ('2019_05_01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2019_05_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019-05-01')::timestamp
      AND (created_time)::timestamp < ('2019-06-01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2019_06_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019-06-01')::timestamp
      AND (created_time)::timestamp < ('2019-07-01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2019_07_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019-07-01')::timestamp
      AND (created_time)::timestamp < ('2019-08-01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2019_08_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019-08-01')::timestamp
      AND (created_time)::timestamp < ('2019-09-01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
    UNION
    SELECT CAST('2019_09_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019-09-01')::timestamp
      AND (created_time)::timestamp < ('2019-10-01')::timestamp
      AND 47<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=49
);

create table fediq_team.novochahtinsk_order_count201909 as (
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_01_01' <= dttm
      and dttm <= '2018_02_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_02_01' <= dttm
      and dttm <= '2018_03_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_03_01' <= dttm
      and dttm <= '2018_04_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_04_01' <= dttm
      and dttm <= '2018_05_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_05_01' <= dttm
      and dttm <= '2018_06_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_06_01' <= dttm
      and dttm <= '2018_07_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_07_01' <= dttm
      and dttm <= '2018_08_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_08_01' <= dttm
      and dttm <= '2018_09_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_09_01' <= dttm
      and dttm <= '2018_10_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_10_01' <= dttm
      and dttm <= '2018_11_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_11_01' <= dttm
      and dttm <= '2018_12_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_12_01' <= dttm
      and dttm <= '2019_01_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2019_01_01' <= dttm
      and dttm <= '2019_02_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2019_02_01' <= dttm
      and dttm <= '2019_03_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2019_03_01' <= dttm
      and dttm <= '2019_04_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2019_04_01' <= dttm
      and dttm <= '2019_05_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2019_05_01' <= dttm
      and dttm <= '2019_06_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2019_06_01' <= dttm
      and dttm <= '2019_07_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2019_07_01' <= dttm
      and dttm <= '2019_08_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2019_08_01' <= dttm
      and dttm <= '2019_09_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2019_09_01' <= dttm
      and dttm <= '2019_10_01'
);

create table fediq_team.novochahtinsk_drivers_count_201909 as (
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_01_01' <= dttm
      and dttm <= '2018_02_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_02_01' <= dttm
      and dttm <= '2018_03_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_03_01' <= dttm
      and dttm <= '2018_04_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_04_01' <= dttm
      and dttm <= '2018_05_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_05_01' <= dttm
      and dttm <= '2018_06_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_06_01' <= dttm
      and dttm <= '2018_07_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_07_01' <= dttm
      and dttm <= '2018_08_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_08_01' <= dttm
      and dttm <= '2018_09_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_09_01' <= dttm
      and dttm <= '2018_10_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_10_01' <= dttm
      and dttm <= '2018_11_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_11_01' <= dttm
      and dttm <= '2018_12_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_12_01' <= dttm
      and dttm <= '2019_01_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2019_01_01' <= dttm
      and dttm <= '2019_02_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2019_02_01' <= dttm
      and dttm <= '2019_03_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2019_03_01' <= dttm
      and dttm <= '2019_04_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2019_04_01' <= dttm
      and dttm <= '2019_05_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2019_05_01' <= dttm
      and dttm <= '2019_06_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2019_06_01' <= dttm
      and dttm <= '2019_07_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2019_07_01' <= dttm
      and dttm <= '2019_08_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2019_08_01' <= dttm
      and dttm <= '2019_09_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2019_09_01' <= dttm
      and dttm <= '2019_10_01'
);

create table fediq_team.novochahtinsk_active_2_201909 as (
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_01_01' <= dttm
            and dttm <= '2018_02_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_02_01' <= dttm
            and dttm <= '2018_03_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_03_01' <= dttm
            and dttm <= '2018_04_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_04_01' <= dttm
            and dttm <= '2018_05_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_05_01' <= dttm
            and dttm <= '2018_06_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_06_01' <= dttm
            and dttm <= '2018_07_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_07_01' <= dttm
            and dttm <= '2018_08_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_08_01' <= dttm
            and dttm <= '2018_09_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_09_01' <= dttm
            and dttm <= '2018_10_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_10_01' <= dttm
            and dttm <= '2018_11_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_11_01' <= dttm
            and dttm <= '2018_12_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_12_01' <= dttm
            and dttm <= '2019_01_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_01_01' <= dttm
            and dttm <= '2019_02_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_02_01' <= dttm
            and dttm <= '2019_03_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_03_01' <= dttm
            and dttm <= '2019_04_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_04_01' <= dttm
            and dttm <= '2019_05_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_05_01' <= dttm
            and dttm <= '2019_06_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_06_01' <= dttm
            and dttm <= '2019_07_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_07_01' <= dttm
            and dttm <= '2019_08_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_08_01' <= dttm
            and dttm <= '2019_09_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_09_01' <= dttm
            and dttm <= '2019_10_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
);

create table fediq_team.novochahtinsk_active_10_201909 as (
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_01_01' <= dttm
            and dttm <= '2018_02_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_02_01' <= dttm
            and dttm <= '2018_03_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_03_01' <= dttm
            and dttm <= '2018_04_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_04_01' <= dttm
            and dttm <= '2018_05_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_05_01' <= dttm
            and dttm <= '2018_06_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_06_01' <= dttm
            and dttm <= '2018_07_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_07_01' <= dttm
            and dttm <= '2018_08_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_08_01' <= dttm
            and dttm <= '2018_09_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_09_01' <= dttm
            and dttm <= '2018_10_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_10_01' <= dttm
            and dttm <= '2018_11_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_11_01' <= dttm
            and dttm <= '2018_12_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_12_01' <= dttm
            and dttm <= '2019_01_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_01_01' <= dttm
            and dttm <= '2019_02_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_02_01' <= dttm
            and dttm <= '2019_03_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_03_01' <= dttm
            and dttm <= '2019_04_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_04_01' <= dttm
            and dttm <= '2019_05_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_05_01' <= dttm
            and dttm <= '2019_06_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_06_01' <= dttm
            and dttm <= '2019_07_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_07_01' <= dttm
            and dttm <= '2019_08_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_08_01' <= dttm
            and dttm <= '2019_09_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_09_01' <= dttm
            and dttm <= '2019_10_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
);

create table fediq_team.novochahtinsk_new_drivers_201909 as (
    SELECT CAST('2018_01_01' AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2018_01_01'
            AND dttm < '2018_02_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2018_01_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_02_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2018_02_01'
            AND dttm < '2018_03_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2018_02_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_03_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2018_03_01'
            AND dttm < '2018_04_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2018_03_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_04_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2018_04_01'
            AND dttm < '2018_05_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2018_04_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_05_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2018_05_01'
            AND dttm < '2018_06_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2018_05_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_06_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2018_06_01'
            AND dttm < '2018_07_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2018_06_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_07_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2018_07_01'
            AND dttm < '2018_08_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2018_07_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_08_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2018_08_01'
            AND dttm < '2018_09_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2018_08_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_09_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2018_09_01'
            AND dttm < '2018_10_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2018_09_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_10_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2018_10_01'
            AND dttm < '2018_11_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2018_10_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_11_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2018_11_01'
            AND dttm < '2018_12_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2018_11_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_12_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2018_12_01'
            AND dttm < '2019_01_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2018_12_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019_01_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2019_01_01'
            AND dttm < '2019_02_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2019_01_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019_02_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2019_02_01'
            AND dttm < '2019_03_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2019_02_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019_03_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2019_03_01'
            AND dttm < '2019_04_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2019_03_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019_04_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2019_04_01'
            AND dttm < '2019_05_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2019_04_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019-05-01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2019-05-01'
            AND dttm < '2019-06-01'
            AND status = 'complete'
              EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2019-05-01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019-06-01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2019-06-01'
            AND dttm < '2019-07-01'
            AND status = 'complete'
              EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2019-06-01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019-07-01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2019-07-01'
            AND dttm < '2019-08-01'
            AND status = 'complete'
              EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2019-07-01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019-08-01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2019-08-01'
            AND dttm < '2019-09-01'
            AND status = 'complete'
              EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2019-08-01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019-09-01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm >= '2019-09-01'
            AND dttm < '2019-10-01'
            AND status = 'complete'
              EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE dttm < '2019-09-01'
            AND status = 'complete') as A
);

create table fediq_team.novochahtinsk_income_by_class as (
    SELECT min(dttm), round(sum(order_cost)) * 0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_01_01' <= dttm
      and dttm <= '2018_02_01'
    UNION
    SELECT min(dttm),  round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_02_01' <= dttm
      and dttm <= '2018_03_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_03_01' <= dttm
      and dttm <= '2018_04_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_04_01' <= dttm
      and dttm <= '2018_05_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_05_01' <= dttm
      and dttm <= '2018_06_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_06_01' <= dttm
      and dttm <= '2018_07_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_07_01' <= dttm
      and dttm <= '2018_08_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_08_01' <= dttm
      and dttm <= '2018_09_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_09_01' <= dttm
      and dttm <= '2018_10_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_10_01' <= dttm
      and dttm <= '2018_11_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_11_01' <= dttm
      and dttm <= '2018_12_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_12_01' <= dttm
      and dttm <= '2019_01_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_01_01' <= dttm
      and dttm <= '2019_02_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_02_01' <= dttm
      and dttm <= '2019_03_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_03_01' <= dttm
      and dttm <= '2019_04_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_04_01' <= dttm
      and dttm <= '2019_05_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_05_01' <= dttm
      and dttm <= '2019_06_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_06_01' <= dttm
      and dttm <= '2019_07_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_07_01' <= dttm
      and dttm <= '2019_08_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_08_01' <= dttm
      and dttm <= '2019_09_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_09_01' <= dttm
      and dttm <= '2019_10_01'
);

ALTER TABLE fediq_team.novochahtinsk_income_by_class_201909 ADD COLUMN expenses integer DEFAULT 650000;


create table fediq_team.novochahtinsk_GMV_201909 as (
    SELECT min, sum(gmv_by_car) as GMV FROM (
        SELECT min, car_class, mean_check * count as GMV_by_car FROM fediq_team.novochahtinsk_mean_check_201909
    ) AS T
    GROUP BY min
);

create table fediq_team.novochantinsk_amount_treap_201909 as(
    SELECT min, sum(count)
    FROM fediq_team.novochahtinsk_mean_check_201909
    GROUP BY min
);

create table fediq_team.novochahtinsk_new_client_201909 as(
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2018-01-01' and dttm < '2018-02-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2018-01-01'and status = 'complete')
    select count(*), ('2018-01-01')::timestamp
    from p
);

insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2018-02-01' and dttm < '2018-03-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2018-02-01'and status = 'complete')
    select count(*), ('2018-02-01')::timestamp
    from p
);

insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2018-03-01' and dttm < '2018-04-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2018-03-01'and status = 'complete')
    select count(*), ('2018-03-01')::timestamp
    from p
);
insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2018-04-01' and dttm < '2018-05-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2018-04-01'and status = 'complete')
    select count(*), ('2018-04-01')::timestamp
    from p
);
insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2018-05-01' and dttm < '2018-06-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2018-05-01'and status = 'complete')
    select count(*), ('2018-05-01')::timestamp
    from p
);
insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2018-06-01' and dttm < '2018-07-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2018-06-01'and status = 'complete')
    select count(*), ('2018-06-01')::timestamp
    from p
);
insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2018-07-01' and dttm < '2018-08-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2018-07-01'and status = 'complete')
    select count(*), ('2018-07-01')::timestamp
    from p
);
insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2018-08-01' and dttm < '2018-09-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2018-08-01'and status = 'complete')
    select count(*), ('2018-08-01')::timestamp
    from p
);
insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2018-09-01' and dttm < '2018-10-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2018-09-01'and status = 'complete')
    select count(*), ('2018-09-01')::timestamp
    from p
);
insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2018-10-01' and dttm < '2018-11-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2018-10-01'and status = 'complete')
    select count(*), ('2018-10-01')::timestamp
    from p
);
insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2018-11-01' and dttm < '2018-12-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2018-11-01'and status = 'complete')
    select count(*), ('2018-11-01')::timestamp
    from p
);
insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2018-12-01' and dttm < '2019-01-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2018-12-01'and status = 'complete')
    select count(*), ('2018-12-01')::timestamp
    from p
);
insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2019-01-01' and dttm < '2019-02-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2019-01-01'and status = 'complete')
    select count(*), ('2019-01-01')::timestamp
    from p
);
insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2019-02-01' and dttm < '2019-03-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2019-02-01'and status = 'complete')
    select count(*), ('2019-02-01')::timestamp
    from p
);
insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2019-03-01' and dttm < '2019-04-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2019-03-01'and status = 'complete')
    select count(*), ('2019-03-01')::timestamp
    from p
);
insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2019-04-01' and dttm < '2019-05-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2019-04-01'and status = 'complete')
    select count(*), ('2019-04-01')::timestamp
    from p
);
insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2019-05-01' and dttm < '2019-06-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2019-05-01'and status = 'complete')
    select count(*), ('2019-05-01')::timestamp
    from p
);
insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2019-06-01' and dttm < '2019-07-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2019-06-01'and status = 'complete')
    select count(*), ('2019-06-01')::timestamp
    from p
);
insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2019-07-01' and dttm < '2019-08-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2019-07-01'and status = 'complete')
    select count(*), ('2019-07-01')::timestamp
    from p
);
insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2019-08-01' and dttm < '2019-09-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2019-08-01'and status = 'complete')
    select count(*), ('2019-08-01')::timestamp
    from p
);
insert into fediq_team.novochahtinsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm >= '2019-09-01' and dttm < '2019-10-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.novochahtinsk_orders_201909
    where dttm < '2019-09-01'and status = 'complete')
    select count(*), ('2019-09-01')::timestamp
    from p
);

create table fediq_team.novochahtinsk_kernel_201909 as (
    with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-02-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-01-01' and cnt >= 4)
    select count(*), ('2018-02-01')::timestamp
from t
);
insert into fediq_team.novochahtinsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-03-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-02-01' and cnt >= 4)
    select count(*), ('2018-03-01')::timestamp
from t
);
insert into fediq_team.novochahtinsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-04-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-03-01' and cnt >= 4)
    select count(*), ('2018-04-01')::timestamp
from t
);
insert into fediq_team.novochahtinsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-05-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-04-01' and cnt >= 4)
    select count(*), ('2018-05-01')::timestamp
from t
);
insert into fediq_team.novochahtinsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-06-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-05-01' and cnt >= 4)
    select count(*), ('2018-06-01')::timestamp
from t
);
insert into fediq_team.novochahtinsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-07-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-06-01' and cnt >= 4)
    select count(*), ('2018-07-01')::timestamp
from t
);
insert into fediq_team.novochahtinsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-08-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-07-01' and cnt >= 4)
    select count(*), ('2018-08-01')::timestamp
from t
);
insert into fediq_team.novochahtinsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-09-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-08-01' and cnt >= 4)
    select count(*), ('2018-09-01')::timestamp
from t
);
insert into fediq_team.novochahtinsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-10-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-09-01' and cnt >= 4)
    select count(*), ('2018-10-01')::timestamp
from t
);
insert into fediq_team.novochahtinsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-11-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-10-01' and cnt >= 4)
    select count(*), ('2018-11-01')::timestamp
from t
);
insert into fediq_team.novochahtinsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-12-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-11-01' and cnt >= 4)
    select count(*), ('2018-12-01')::timestamp
from t
);
insert into fediq_team.novochahtinsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-01-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-12-01' and cnt >= 4)
    select count(*), ('2019-01-01')::timestamp
from t
);
insert into fediq_team.novochahtinsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-02-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-01-01' and cnt >= 4)
    select count(*), ('2019-02-01')::timestamp
from t
);
insert into fediq_team.novochahtinsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-03-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-02-01' and cnt >= 4)
    select count(*), ('2019-03-01')::timestamp
from t
);
insert into fediq_team.novochahtinsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-04-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-03-01' and cnt >= 4)
    select count(*), ('2019-04-01')::timestamp
from t
);
insert into fediq_team.novochahtinsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-05-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-04-01' and cnt >= 4)
    select count(*), ('2019-05-01')::timestamp
from t
);
insert into fediq_team.novochahtinsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-06-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-05-01' and cnt >= 4)
    select count(*), ('2019-06-01')::timestamp
from t
);
insert into fediq_team.novochahtinsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-07-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-06-01' and cnt >= 4)
    select count(*), ('2019-07-01')::timestamp
from t
);
insert into fediq_team.novochahtinsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-08-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-07-01' and cnt >= 4)
    select count(*), ('2019-08-01')::timestamp
from t
);
insert into fediq_team.novochahtinsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.novochahtinsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-09-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-08-01' and cnt >= 4)
    select count(*), ('2019-09-01')::timestamp
from t
);

create table fediq_team.novochahtinsk_old_client_201909 as(
    SELECT month, mau - count as old_pay
    FROM fediq_team.novochahtinsk_mau_201909
    INNER JOIN fediq_team.novochahtinsk_new_client_201909 ON month = timestamp
);


create table fediq_team.rettenrion_novochahtinsk_201909 as(
    with p as(
        select *
        from (select mau - count as users_end, date_trunc('month', timestamp - INTERVAL '365' DAY TO SECOND) as dttm
        from fediq_team.novochahtinsk_mau_201909
        inner join fediq_team.novochahtinsk_new_client_201909 on timestamp = month) as T
        inner join fediq_team.novochahtinsk_mau_201909 on T.dttm = month
        )
    select (users_end)::float / (mau)::float as rettention, dttm
    from p
);

create table fediq_team.rettenrion_Nijnii_tagil_201909 as(
    with p as(
        select *
        from (select mau - count as users_end, date_trunc('month', timestamp - INTERVAL '365' DAY TO SECOND) as dttm
        from fediq_team.Nijnii_tagil_mau_201909
        inner join fediq_team.Nijnii_tagil_new_client_201909 on timestamp = month) as T
        inner join fediq_team.Nijnii_tagil_mau_201909 on T.dttm = month
        )
    select (users_end)::float / (mau)::float as rettention, dttm
    from p
);

create table fediq_team.rettenrion_arhangelsk_201909 as(
    with p as(
        select *
        from (select mau - count as users_end, date_trunc('month', timestamp - INTERVAL '365' DAY TO SECOND) as dttm
        from fediq_team.arhangelsk_mau_201909
        inner join fediq_team.arhangelsk_new_client_201909 on timestamp = month) as T
        inner join fediq_team.arhangelsk_mau_201909 on T.dttm = month
        )
    select (users_end)::float / (mau)::float as rettention, dttm
    from p
);

--TAGIL--
create table fediq_team.Nijnii_tagil_orders_201909 as(
    SELECT *
    FROM fediq_team.orders_table_201909
    WHERE 57<=start_point_a_lat and start_point_a_lat <= 59
);

create table fediq_team.Nijnii_tagil_mau_201909 as(
with p as (select passenger_id, date_trunc('month', dttm) as month from fediq_team.Nijnii_tagil_orders_201909 where status = 'complete')
select count(distinct passenger_id) as MAU, month
from p
group by(month));

create table fediq_team.Nijnii_tagil_dau_201909 as(
with p as (select passenger_id, date_trunc('day', dttm) as day from fediq_team.Nijnii_tagil_orders_201909 where status='complete')
select count(distinct passenger_id) as DAU, day
from p
group by(day));

create table fediq_team.Nijnii_tagil_rettention_2019_09 as(with sum_of_dau as(
with p as (select dau, date_trunc('month', day) as month from fediq_team.Nijnii_tagil_dau_201909)
select sum(dau) as sum_of_dau, month
from p
group by(month))
    select 1.0*sum_of_dau / mau as rettention, fediq_team.Nijnii_tagil_mau_201909.month as month from sum_of_dau, fediq_team.Nijnii_tagil_mau_201909
where sum_of_dau.month = fediq_team.Nijnii_tagil_mau_201909.month);

create table fediq_team.Nijnii_tagil_dolya_ottoka_201909 as (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and dttm < '2018-02-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and dttm < '2018-02-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and dttm < '2018-03-01' and dttm >= '2018-02-01 00:00:00'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-02-01')::timestamp as month from j, l);

insert into fediq_team.Nijnii_tagil_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-02-01' <= dttm and dttm < '2018-03-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-02-01' <= dttm and dttm < '2018-03-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-03-01' <= dttm and dttm < '2018-04-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-03-01')::timestamp as month from j, l);

insert into fediq_team.Nijnii_tagil_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-03-01' <= dttm and dttm < '2018-04-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-03-01' <= dttm and dttm < '2018-04-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-04-01' <= dttm and dttm < '2018-05-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-04-01')::timestamp as month from j, l);

insert into fediq_team.Nijnii_tagil_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-04-01' <= dttm and dttm < '2018-05-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-04-01' <= dttm and dttm < '2018-05-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-05-01' <= dttm and dttm < '2018-06-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-05-01')::timestamp as month from j, l);

insert into fediq_team.Nijnii_tagil_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-05-01' <= dttm and dttm < '2018-06-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-05-01' <= dttm and dttm < '2018-06-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-06-01' <= dttm and dttm < '2018-07-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-06-01')::timestamp as month from j, l);

insert into fediq_team.Nijnii_tagil_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-06-01' <= dttm and dttm < '2018-07-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-06-01' <= dttm and dttm < '2018-07-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-07-01' <= dttm and dttm < '2018-08-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-07-01')::timestamp as month from j, l);

insert into fediq_team.Nijnii_tagil_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-07-01' <= dttm and dttm < '2018-08-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-07-01' <= dttm and dttm < '2018-08-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-08-01' <= dttm and dttm < '2018-09-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-08-01')::timestamp as month from j, l);

insert into fediq_team.Nijnii_tagil_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-08-01' <= dttm and dttm < '2018-09-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-08-01' <= dttm and dttm < '2018-09-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-09-01' <= dttm and dttm < '2018-10-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-09-01')::timestamp as month from j, l);

insert into fediq_team.Nijnii_tagil_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-09-01' <= dttm and dttm < '2018-10-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-09-01' <= dttm and dttm < '2018-10-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-10-01' <= dttm and dttm < '2018-11-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-10-01')::timestamp as month from j, l);

insert into fediq_team.Nijnii_tagil_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-10-01' <= dttm and dttm < '2018-11-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-10-01' <= dttm and dttm < '2018-11-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-11-01' <= dttm and dttm < '2018-12-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-11-01')::timestamp as month from j, l);

insert into fediq_team.Nijnii_tagil_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-11-01' <= dttm and dttm < '2018-12-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-11-01' <= dttm and dttm < '2018-12-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-12-01' <= dttm and dttm < '2019-01-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-12-01')::timestamp as month from j, l);

insert into fediq_team.Nijnii_tagil_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-12-01' <= dttm and dttm < '2019-01-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2018-12-01' <= dttm and dttm < '2019-01-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-01-01' <= dttm and dttm < '2019-02-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-01-01')::timestamp as month from j, l);

insert into fediq_team.Nijnii_tagil_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-01-01' <= dttm and dttm < '2019-02-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-01-01' <= dttm and dttm < '2019-02-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-02-01' <= dttm and dttm < '2019-03-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-02-01')::timestamp as month from j, l);

insert into fediq_team.Nijnii_tagil_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-02-01' <= dttm and dttm < '2019-03-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-02-01' <= dttm and dttm < '2019-03-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-03-01' <= dttm and dttm < '2019-04-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-03-01')::timestamp as month from j, l);

insert into fediq_team.Nijnii_tagil_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-03-01' <= dttm and dttm < '2019-04-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-03-01' <= dttm and dttm < '2019-04-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-04-01' <= dttm and dttm < '2019-05-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-04-01')::timestamp as month from j, l);

insert into fediq_team.Nijnii_tagil_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-04-01' <= dttm and dttm < '2019-05-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-04-01' <= dttm and dttm < '2019-05-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-05-01' <= dttm and dttm < '2019-06-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-05-01')::timestamp as month from j, l)

insert into fediq_team.Nijnii_tagil_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-05-01' <= dttm and dttm < '2019-06-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-05-01' <= dttm and dttm < '2019-06-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-06-01' <= dttm and dttm < '2019-07-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-06-01')::timestamp as month from j, l)

insert into fediq_team.Nijnii_tagil_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-06-01' <= dttm and dttm < '2019-07-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-06-01' <= dttm and dttm < '2019-07-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-07-01' <= dttm and dttm < '2019-08-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-07-01')::timestamp as month from j, l);

insert into fediq_team.Nijnii_tagil_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-07-01' <= dttm and dttm < '2019-08-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-07-01' <= dttm and dttm < '2019-08-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-08-01' <= dttm and dttm < '2019-09-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-08-01')::timestamp as month from j, l);

insert into fediq_team.Nijnii_tagil_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-08-01' <= dttm and dttm < '2019-09-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-08-01' <= dttm and dttm < '2019-09-01'
except
select distinct(passenger_id) from fediq_team.Nijnii_tagil_orders_201909
    where status = 'complete' and '2019-09-01' <= dttm and dttm < '2019-10-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-09-01')::timestamp as month from j, l);


create table fediq_team.Nijnii_tagil_rides_count_201909 as (
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_01_01' <= dttm
      and dttm <= '2018_02_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_02_01' <= dttm
      and dttm <= '2018_03_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_03_01' <= dttm
      and dttm <= '2018_04_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_04_01' <= dttm
      and dttm <= '2018_05_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_05_01' <= dttm
      and dttm <= '2018_06_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_06_01' <= dttm
      and dttm <= '2018_07_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_07_01' <= dttm
      and dttm <= '2018_08_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_08_01' <= dttm
      and dttm <= '2018_09_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_09_01' <= dttm
      and dttm <= '2018_10_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_10_01' <= dttm
      and dttm <= '2018_11_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_11_01' <= dttm
      and dttm <= '2018_12_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_12_01' <= dttm
      and dttm <= '2019_01_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_01_01' <= dttm
      and dttm <= '2019_02_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_02_01' <= dttm
      and dttm <= '2019_03_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_03_01' <= dttm
      and dttm <= '2019_04_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_04_01' <= dttm
      and dttm <= '2019_05_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_05_01' <= dttm
      and dttm <= '2019_06_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_06_01' <= dttm
      and dttm <= '2019_07_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_07_01' <= dttm
      and dttm <= '2019_08_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_08_01' <= dttm
      and dttm <= '2019_09_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_09_01' <= dttm
      and dttm <= '2019_10_01'
    GROUP BY car_class
);

SELECT dttm, car_class, order_cost, order_id
FROM fediq_team.nijnii_tagil_orders_201909
WHERE status = 'complete'
    and dttm IS NOT NULL
    and '2018_10_01' <= dttm
    and dttm <= '2018_11_01'
Order By order_cost DESC

create table fediq_team.nijnii_tagil_mean_check_201909 as (
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_01_01' <= dttm
      and dttm <= '2018_02_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_02_01' <= dttm
      and dttm <= '2018_03_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_03_01' <= dttm
      and dttm <= '2018_04_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_04_01' <= dttm
      and dttm <= '2018_05_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_05_01' <= dttm
      and dttm <= '2018_06_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_06_01' <= dttm
      and dttm <= '2018_07_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_07_01' <= dttm
      and dttm <= '2018_08_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_08_01' <= dttm
      and dttm <= '2018_09_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_09_01' <= dttm
      and dttm <= '2018_10_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_10_01' <= dttm
      and dttm <= '2018_11_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_11_01' <= dttm
      and dttm <= '2018_12_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_12_01' <= dttm
      and dttm <= '2019_01_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_01_01' <= dttm
      and dttm <= '2019_02_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_02_01' <= dttm
      and dttm <= '2019_03_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_03_01' <= dttm
      and dttm <= '2019_04_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_04_01' <= dttm
      and dttm <= '2019_05_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_05_01' <= dttm
      and dttm <= '2019_06_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_06_01' <= dttm
      and dttm <= '2019_07_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_07_01' <= dttm
      and dttm <= '2019_08_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_08_01' <= dttm
      and dttm <= '2019_09_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_09_01' <= dttm
      and dttm <= '2019_10_01'
    GROUP BY car_class
);

create table fediq_team.nijnii_tagil_offers_count_201909 as (
    SELECT CAST('2018_01_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_01_01')::timestamp
      AND (created_time)::timestamp < ('2018_02_01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2018_02_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_02_01')::timestamp
      AND (created_time)::timestamp < ('2018_03_01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2018_03_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_03_01')::timestamp
      AND (created_time)::timestamp < ('2018_04_01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2018_04_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_04_01')::timestamp
      AND (created_time)::timestamp < ('2018_05_01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2018_05_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_05_01')::timestamp
      AND (created_time)::timestamp < ('2018_06_01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2018_06_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_06_01')::timestamp
      AND (created_time)::timestamp < ('2018_07_01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2018_07_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_07_01')::timestamp
      AND (created_time)::timestamp < ('2018_08_01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2018_08_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_08_01')::timestamp
      AND (created_time)::timestamp < ('2018_09_01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2018_09_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_09_01')::timestamp
      AND (created_time)::timestamp < ('2018_10_01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2018_10_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_10_01')::timestamp
      AND (created_time)::timestamp < ('2018_11_01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2018_11_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_11_01')::timestamp
      AND (created_time)::timestamp < ('2018_12_01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2018_12_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_12_01')::timestamp
      AND (created_time)::timestamp < ('2019_01_01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2019_01_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019_01_01')::timestamp
      AND (created_time)::timestamp < ('2019_02_01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2019_02_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019_02_01')::timestamp
      AND (created_time)::timestamp < ('2019_03_01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2019_03_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019_03_01')::timestamp
      AND (created_time)::timestamp < ('2019_04_01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2019_04_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019_04_01')::timestamp
      AND (created_time)::timestamp < ('2019_05_01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2019_05_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019-05-01')::timestamp
      AND (created_time)::timestamp < ('2019-06-01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2019_06_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019-06-01')::timestamp
      AND (created_time)::timestamp < ('2019-07-01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2019_07_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019-07-01')::timestamp
      AND (created_time)::timestamp < ('2019-08-01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2019_08_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019-08-01')::timestamp
      AND (created_time)::timestamp < ('2019-09-01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2019_09_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019-09-01')::timestamp
      AND (created_time)::timestamp < ('2019-10-01')::timestamp
      AND 57<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
);

create table fediq_team.nijnii_tagil_order_count201909 as (
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_01_01' <= dttm
      and dttm <= '2018_02_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_02_01' <= dttm
      and dttm <= '2018_03_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_03_01' <= dttm
      and dttm <= '2018_04_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_04_01' <= dttm
      and dttm <= '2018_05_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_05_01' <= dttm
      and dttm <= '2018_06_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_06_01' <= dttm
      and dttm <= '2018_07_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_07_01' <= dttm
      and dttm <= '2018_08_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_08_01' <= dttm
      and dttm <= '2018_09_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_09_01' <= dttm
      and dttm <= '2018_10_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_10_01' <= dttm
      and dttm <= '2018_11_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_11_01' <= dttm
      and dttm <= '2018_12_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_12_01' <= dttm
      and dttm <= '2019_01_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2019_01_01' <= dttm
      and dttm <= '2019_02_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2019_02_01' <= dttm
      and dttm <= '2019_03_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2019_03_01' <= dttm
      and dttm <= '2019_04_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2019_04_01' <= dttm
      and dttm <= '2019_05_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2019_05_01' <= dttm
      and dttm <= '2019_06_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2019_06_01' <= dttm
      and dttm <= '2019_07_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2019_07_01' <= dttm
      and dttm <= '2019_08_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2019_08_01' <= dttm
      and dttm <= '2019_09_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2019_09_01' <= dttm
      and dttm <= '2019_10_01'
);

create table fediq_team.nijnii_tagil_drivers_count_201909 as (
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_01_01' <= dttm
      and dttm <= '2018_02_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_02_01' <= dttm
      and dttm <= '2018_03_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_03_01' <= dttm
      and dttm <= '2018_04_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_04_01' <= dttm
      and dttm <= '2018_05_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_05_01' <= dttm
      and dttm <= '2018_06_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_06_01' <= dttm
      and dttm <= '2018_07_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_07_01' <= dttm
      and dttm <= '2018_08_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_08_01' <= dttm
      and dttm <= '2018_09_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_09_01' <= dttm
      and dttm <= '2018_10_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_10_01' <= dttm
      and dttm <= '2018_11_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_11_01' <= dttm
      and dttm <= '2018_12_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.novochahtinsk_orders_201909
    WHERE '2018_12_01' <= dttm
      and dttm <= '2019_01_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2019_01_01' <= dttm
      and dttm <= '2019_02_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2019_02_01' <= dttm
      and dttm <= '2019_03_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2019_03_01' <= dttm
      and dttm <= '2019_04_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2019_04_01' <= dttm
      and dttm <= '2019_05_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2019_05_01' <= dttm
      and dttm <= '2019_06_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2019_06_01' <= dttm
      and dttm <= '2019_07_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2019_07_01' <= dttm
      and dttm <= '2019_08_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2019_08_01' <= dttm
      and dttm <= '2019_09_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2019_09_01' <= dttm
      and dttm <= '2019_10_01'
);

create table fediq_team.nijnii_tagil_active_2_201909 as (
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_01_01' <= dttm
            and dttm <= '2018_02_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_02_01' <= dttm
            and dttm <= '2018_03_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_03_01' <= dttm
            and dttm <= '2018_04_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_04_01' <= dttm
            and dttm <= '2018_05_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_05_01' <= dttm
            and dttm <= '2018_06_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_06_01' <= dttm
            and dttm <= '2018_07_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_07_01' <= dttm
            and dttm <= '2018_08_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_08_01' <= dttm
            and dttm <= '2018_09_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_09_01' <= dttm
            and dttm <= '2018_10_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_10_01' <= dttm
            and dttm <= '2018_11_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_11_01' <= dttm
            and dttm <= '2018_12_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_12_01' <= dttm
            and dttm <= '2019_01_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2019_01_01' <= dttm
            and dttm <= '2019_02_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2019_02_01' <= dttm
            and dttm <= '2019_03_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2019_03_01' <= dttm
            and dttm <= '2019_04_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2019_04_01' <= dttm
            and dttm <= '2019_05_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2019_05_01' <= dttm
            and dttm <= '2019_06_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2019_06_01' <= dttm
            and dttm <= '2019_07_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2019_07_01' <= dttm
            and dttm <= '2019_08_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2019_08_01' <= dttm
            and dttm <= '2019_09_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2019_09_01' <= dttm
            and dttm <= '2019_10_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
);

create table fediq_team.nijnii_tagil_active_10_201909 as (
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_01_01' <= dttm
            and dttm <= '2018_02_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_02_01' <= dttm
            and dttm <= '2018_03_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_03_01' <= dttm
            and dttm <= '2018_04_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_04_01' <= dttm
            and dttm <= '2018_05_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_05_01' <= dttm
            and dttm <= '2018_06_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_06_01' <= dttm
            and dttm <= '2018_07_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_07_01' <= dttm
            and dttm <= '2018_08_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_08_01' <= dttm
            and dttm <= '2018_09_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_09_01' <= dttm
            and dttm <= '2018_10_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_10_01' <= dttm
            and dttm <= '2018_11_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2018_11_01' <= dttm
            and dttm <= '2018_12_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2018_12_01' <= dttm
            and dttm <= '2019_01_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2019_01_01' <= dttm
            and dttm <= '2019_02_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2019_02_01' <= dttm
            and dttm <= '2019_03_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_03_01' <= dttm
            and dttm <= '2019_04_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2019_04_01' <= dttm
            and dttm <= '2019_05_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2019_05_01' <= dttm
            and dttm <= '2019_06_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.novochahtinsk_orders_201909
          WHERE '2019_06_01' <= dttm
            and dttm <= '2019_07_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2019_07_01' <= dttm
            and dttm <= '2019_08_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2019_08_01' <= dttm
            and dttm <= '2019_09_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE '2019_09_01' <= dttm
            and dttm <= '2019_10_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
);

create table fediq_team.nijnii_tagil_new_drivers_201909 as (
    SELECT CAST('2018_01_01' AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2018_01_01'
            AND dttm < '2018_02_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2018_01_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_02_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2018_02_01'
            AND dttm < '2018_03_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2018_02_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_03_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2018_03_01'
            AND dttm < '2018_04_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2018_03_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_04_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2018_04_01'
            AND dttm < '2018_05_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2018_04_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_05_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2018_05_01'
            AND dttm < '2018_06_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2018_05_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_06_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2018_06_01'
            AND dttm < '2018_07_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2018_06_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_07_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2018_07_01'
            AND dttm < '2018_08_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2018_07_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_08_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2018_08_01'
            AND dttm < '2018_09_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2018_08_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_09_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2018_09_01'
            AND dttm < '2018_10_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2018_09_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_10_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2018_10_01'
            AND dttm < '2018_11_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2018_10_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_11_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2018_11_01'
            AND dttm < '2018_12_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2018_11_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_12_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2018_12_01'
            AND dttm < '2019_01_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2018_12_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019_01_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2019_01_01'
            AND dttm < '2019_02_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2019_01_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019_02_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2019_02_01'
            AND dttm < '2019_03_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2019_02_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019_03_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2019_03_01'
            AND dttm < '2019_04_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2019_03_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019_04_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2019_04_01'
            AND dttm < '2019_05_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2019_04_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019-05-01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2019-05-01'
            AND dttm < '2019-06-01'
            AND status = 'complete'
              EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2019-05-01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019-06-01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2019-06-01'
            AND dttm < '2019-07-01'
            AND status = 'complete'
              EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2019-06-01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019-07-01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2019-07-01'
            AND dttm < '2019-08-01'
            AND status = 'complete'
              EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2019-07-01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019-08-01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2019-08-01'
            AND dttm < '2019-09-01'
            AND status = 'complete'
              EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2019-08-01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019-09-01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm >= '2019-09-01'
            AND dttm < '2019-10-01'
            AND status = 'complete'
              EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.nijnii_tagil_orders_201909
          WHERE dttm < '2019-09-01'
            AND status = 'complete') as A
);

create table fediq_team.nijnii_tagil_income_by_class as (
    SELECT min(dttm), round(sum(order_cost)) * 0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_01_01' <= dttm
      and dttm <= '2018_02_01'
    UNION
    SELECT min(dttm),  round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_02_01' <= dttm
      and dttm <= '2018_03_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_03_01' <= dttm
      and dttm <= '2018_04_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_04_01' <= dttm
      and dttm <= '2018_05_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_05_01' <= dttm
      and dttm <= '2018_06_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_06_01' <= dttm
      and dttm <= '2018_07_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_07_01' <= dttm
      and dttm <= '2018_08_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_08_01' <= dttm
      and dttm <= '2018_09_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_09_01' <= dttm
      and dttm <= '2018_10_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_10_01' <= dttm
      and dttm <= '2018_11_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_11_01' <= dttm
      and dttm <= '2018_12_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_12_01' <= dttm
      and dttm <= '2019_01_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_01_01' <= dttm
      and dttm <= '2019_02_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_02_01' <= dttm
      and dttm <= '2019_03_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_03_01' <= dttm
      and dttm <= '2019_04_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_04_01' <= dttm
      and dttm <= '2019_05_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_05_01' <= dttm
      and dttm <= '2019_06_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_06_01' <= dttm
      and dttm <= '2019_07_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_07_01' <= dttm
      and dttm <= '2019_08_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_08_01' <= dttm
      and dttm <= '2019_09_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_09_01' <= dttm
      and dttm <= '2019_10_01'
);

ALTER TABLE fediq_team.nijnii_tagil_income_by_class ADD COLUMN expenses integer DEFAULT 650000;


create table fediq_team.nijnii_tagil_GMV_201909 as (
    SELECT min, sum(gmv_by_car) as GMV FROM (
        SELECT min, car_class, mean_check * count as GMV_by_car FROM fediq_team.nijnii_tagil_mean_check_201909
    ) AS T
    GROUP BY min
);

create table fediq_team.nijnii_tagil_amount_treap_201909 as(
    SELECT min, sum(count)
    FROM fediq_team.nijnii_tagil_mean_check_201909
    GROUP BY min
);

create table fediq_team.nijnii_tagil_new_client_201909 as(
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2018-01-01' and dttm < '2018-02-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2018-01-01'and status = 'complete')
    select count(*), ('2018-01-01')::timestamp
    from p
);

insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2018-02-01' and dttm < '2018-03-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2018-02-01'and status = 'complete')
    select count(*), ('2018-02-01')::timestamp
    from p
);

insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2018-03-01' and dttm < '2018-04-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2018-03-01'and status = 'complete')
    select count(*), ('2018-03-01')::timestamp
    from p
);
insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2018-04-01' and dttm < '2018-05-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2018-04-01'and status = 'complete')
    select count(*), ('2018-04-01')::timestamp
    from p
);
insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2018-05-01' and dttm < '2018-06-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2018-05-01'and status = 'complete')
    select count(*), ('2018-05-01')::timestamp
    from p
);
insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2018-06-01' and dttm < '2018-07-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2018-06-01'and status = 'complete')
    select count(*), ('2018-06-01')::timestamp
    from p
);
insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2018-07-01' and dttm < '2018-08-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2018-07-01'and status = 'complete')
    select count(*), ('2018-07-01')::timestamp
    from p
);
insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2018-08-01' and dttm < '2018-09-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2018-08-01'and status = 'complete')
    select count(*), ('2018-08-01')::timestamp
    from p
);
insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2018-09-01' and dttm < '2018-10-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2018-09-01'and status = 'complete')
    select count(*), ('2018-09-01')::timestamp
    from p
);
insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2018-10-01' and dttm < '2018-11-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2018-10-01'and status = 'complete')
    select count(*), ('2018-10-01')::timestamp
    from p
);
insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2018-11-01' and dttm < '2018-12-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2018-11-01'and status = 'complete')
    select count(*), ('2018-11-01')::timestamp
    from p
);
insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2018-12-01' and dttm < '2019-01-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2018-12-01'and status = 'complete')
    select count(*), ('2018-12-01')::timestamp
    from p
);
insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2019-01-01' and dttm < '2019-02-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2019-01-01'and status = 'complete')
    select count(*), ('2019-01-01')::timestamp
    from p
);
insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2019-02-01' and dttm < '2019-03-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2019-02-01'and status = 'complete')
    select count(*), ('2019-02-01')::timestamp
    from p
);
insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2019-03-01' and dttm < '2019-04-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2019-03-01'and status = 'complete')
    select count(*), ('2019-03-01')::timestamp
    from p
);
insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2019-04-01' and dttm < '2019-05-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2019-04-01'and status = 'complete')
    select count(*), ('2019-04-01')::timestamp
    from p
);
insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2019-05-01' and dttm < '2019-06-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2019-05-01'and status = 'complete')
    select count(*), ('2019-05-01')::timestamp
    from p
);
insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2019-06-01' and dttm < '2019-07-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2019-06-01'and status = 'complete')
    select count(*), ('2019-06-01')::timestamp
    from p
);
insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2019-07-01' and dttm < '2019-08-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2019-07-01'and status = 'complete')
    select count(*), ('2019-07-01')::timestamp
    from p
);
insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2019-08-01' and dttm < '2019-09-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2019-08-01'and status = 'complete')
    select count(*), ('2019-08-01')::timestamp
    from p
);
insert into fediq_team.nijnii_tagil_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm >= '2019-09-01' and dttm < '2019-10-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.nijnii_tagil_orders_201909
    where dttm < '2019-09-01'and status = 'complete')
    select count(*), ('2019-09-01')::timestamp
    from p
);

create table fediq_team.nijnii_tagil_kernel_201909 as (
    with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-02-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-01-01' and cnt >= 4)
    select count(*), ('2018-02-01')::timestamp
from t
);
insert into fediq_team.nijnii_tagil_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-03-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-02-01' and cnt >= 4)
    select count(*), ('2018-03-01')::timestamp
from t
);
insert into fediq_team.nijnii_tagil_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-04-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-03-01' and cnt >= 4)
    select count(*), ('2018-04-01')::timestamp
from t
);
insert into fediq_team.nijnii_tagil_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-05-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-04-01' and cnt >= 4)
    select count(*), ('2018-05-01')::timestamp
from t
);
insert into fediq_team.nijnii_tagil_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-06-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-05-01' and cnt >= 4)
    select count(*), ('2018-06-01')::timestamp
from t
);
insert into fediq_team.nijnii_tagil_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-07-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-06-01' and cnt >= 4)
    select count(*), ('2018-07-01')::timestamp
from t
);
insert into fediq_team.nijnii_tagil_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-08-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-07-01' and cnt >= 4)
    select count(*), ('2018-08-01')::timestamp
from t
);
insert into fediq_team.nijnii_tagil_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-09-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-08-01' and cnt >= 4)
    select count(*), ('2018-09-01')::timestamp
from t
);
insert into fediq_team.nijnii_tagil_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-10-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-09-01' and cnt >= 4)
    select count(*), ('2018-10-01')::timestamp
from t
);
insert into fediq_team.nijnii_tagil_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-11-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-10-01' and cnt >= 4)
    select count(*), ('2018-11-01')::timestamp
from t
);
insert into fediq_team.nijnii_tagil_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-12-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-11-01' and cnt >= 4)
    select count(*), ('2018-12-01')::timestamp
from t
);
insert into fediq_team.nijnii_tagil_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-01-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-12-01' and cnt >= 4)
    select count(*), ('2019-01-01')::timestamp
from t
);
insert into fediq_team.nijnii_tagil_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-02-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-01-01' and cnt >= 4)
    select count(*), ('2019-02-01')::timestamp
from t
);
insert into fediq_team.nijnii_tagil_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-03-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-02-01' and cnt >= 4)
    select count(*), ('2019-03-01')::timestamp
from t
);
insert into fediq_team.nijnii_tagil_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-04-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-03-01' and cnt >= 4)
    select count(*), ('2019-04-01')::timestamp
from t
);
insert into fediq_team.nijnii_tagil_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-05-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-04-01' and cnt >= 4)
    select count(*), ('2019-05-01')::timestamp
from t
);
insert into fediq_team.nijnii_tagil_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-06-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-05-01' and cnt >= 4)
    select count(*), ('2019-06-01')::timestamp
from t
);
insert into fediq_team.nijnii_tagil_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-07-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-06-01' and cnt >= 4)
    select count(*), ('2019-07-01')::timestamp
from t
);
insert into fediq_team.nijnii_tagil_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-08-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-07-01' and cnt >= 4)
    select count(*), ('2019-08-01')::timestamp
from t
);
insert into fediq_team.nijnii_tagil_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-09-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-08-01' and cnt >= 4)
    select count(*), ('2019-09-01')::timestamp
from t
);

create table fediq_team.nijnii_tagil_old_client_201909 as(
    SELECT month, mau - count as old_pay
    FROM fediq_team.nijnii_tagil_mau_201909
    INNER JOIN fediq_team.nijnii_tagil_new_client_201909 ON month = timestamp
);

--ARHAnglelsk--
create table fediq_team.Arhangelsk_orders_201909 as(
    SELECT *
    FROM fediq_team.orders_table_201909
    WHERE 64<=start_point_a_lat and start_point_a_lat <= 65
);

create table fediq_team.Arhangelsk_mau_201909 as(
with p as (select passenger_id, date_trunc('month', dttm) as month from fediq_team.Arhangelsk_orders_201909 where status = 'complete')
select count(distinct passenger_id) as MAU, month
from p
group by(month));

create table fediq_team.Arhangelsk_dau_201909 as(
with p as (select passenger_id, date_trunc('day', dttm) as day from fediq_team.Arhangelsk_orders_201909 where status='complete')
select count(distinct passenger_id) as DAU, day
from p
group by(day));

create table fediq_team.Arhangelsk_rettention_2019_09 as(with sum_of_dau as(
with p as (select dau, date_trunc('month', day) as month from fediq_team.Arhangelsk_dau_201909)
select sum(dau) as sum_of_dau, month
from p
group by(month))
    select 1.0*sum_of_dau / mau as rettention, fediq_team.Arhangelsk_mau_201909.month as month from sum_of_dau, fediq_team.Arhangelsk_mau_201909
where sum_of_dau.month = fediq_team.Arhangelsk_mau_201909.month);

create table fediq_team.Arhangelsk_dolya_ottoka_201909 as (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and dttm < '2018-02-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and dttm < '2018-02-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and dttm < '2018-03-01' and dttm >= '2018-02-01 00:00:00'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-02-01')::timestamp as month from j, l);

insert into fediq_team.Arhangelsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-02-01' <= dttm and dttm < '2018-03-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-02-01' <= dttm and dttm < '2018-03-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-03-01' <= dttm and dttm < '2018-04-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-03-01')::timestamp as month from j, l);

insert into fediq_team.Arhangelsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-03-01' <= dttm and dttm < '2018-04-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-03-01' <= dttm and dttm < '2018-04-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-04-01' <= dttm and dttm < '2018-05-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-04-01')::timestamp as month from j, l);

insert into fediq_team.Arhangelsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-04-01' <= dttm and dttm < '2018-05-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-04-01' <= dttm and dttm < '2018-05-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-05-01' <= dttm and dttm < '2018-06-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-05-01')::timestamp as month from j, l);

insert into fediq_team.Arhangelsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-05-01' <= dttm and dttm < '2018-06-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-05-01' <= dttm and dttm < '2018-06-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-06-01' <= dttm and dttm < '2018-07-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-06-01')::timestamp as month from j, l);

insert into fediq_team.Arhangelsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-06-01' <= dttm and dttm < '2018-07-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-06-01' <= dttm and dttm < '2018-07-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-07-01' <= dttm and dttm < '2018-08-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-07-01')::timestamp as month from j, l);

insert into fediq_team.Arhangelsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-07-01' <= dttm and dttm < '2018-08-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-07-01' <= dttm and dttm < '2018-08-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-08-01' <= dttm and dttm < '2018-09-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-08-01')::timestamp as month from j, l);

insert into fediq_team.Arhangelsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-08-01' <= dttm and dttm < '2018-09-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-08-01' <= dttm and dttm < '2018-09-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-09-01' <= dttm and dttm < '2018-10-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-09-01')::timestamp as month from j, l);

insert into fediq_team.Arhangelsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-09-01' <= dttm and dttm < '2018-10-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-09-01' <= dttm and dttm < '2018-10-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-10-01' <= dttm and dttm < '2018-11-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-10-01')::timestamp as month from j, l);

insert into fediq_team.Arhangelsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-10-01' <= dttm and dttm < '2018-11-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-10-01' <= dttm and dttm < '2018-11-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-11-01' <= dttm and dttm < '2018-12-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-11-01')::timestamp as month from j, l);

insert into fediq_team.Arhangelsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-11-01' <= dttm and dttm < '2018-12-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-11-01' <= dttm and dttm < '2018-12-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-12-01' <= dttm and dttm < '2019-01-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2018-12-01')::timestamp as month from j, l);

insert into fediq_team.Arhangelsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-12-01' <= dttm and dttm < '2019-01-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2018-12-01' <= dttm and dttm < '2019-01-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-01-01' <= dttm and dttm < '2019-02-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-01-01')::timestamp as month from j, l);

insert into fediq_team.Arhangelsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-01-01' <= dttm and dttm < '2019-02-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-01-01' <= dttm and dttm < '2019-02-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-02-01' <= dttm and dttm < '2019-03-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-02-01')::timestamp as month from j, l);

insert into fediq_team.Arhangelsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-02-01' <= dttm and dttm < '2019-03-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-02-01' <= dttm and dttm < '2019-03-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-03-01' <= dttm and dttm < '2019-04-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-03-01')::timestamp as month from j, l);

insert into fediq_team.Arhangelsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-03-01' <= dttm and dttm < '2019-04-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-03-01' <= dttm and dttm < '2019-04-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-04-01' <= dttm and dttm < '2019-05-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-04-01')::timestamp as month from j, l);

insert into fediq_team.Arhangelsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-04-01' <= dttm and dttm < '2019-05-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-04-01' <= dttm and dttm < '2019-05-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-05-01' <= dttm and dttm < '2019-06-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-05-01')::timestamp as month from j, l)

insert into fediq_team.Arhangelsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-05-01' <= dttm and dttm < '2019-06-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-05-01' <= dttm and dttm < '2019-06-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-06-01' <= dttm and dttm < '2019-07-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-06-01')::timestamp as month from j, l)

insert into fediq_team.Arhangelsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-06-01' <= dttm and dttm < '2019-07-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-06-01' <= dttm and dttm < '2019-07-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-07-01' <= dttm and dttm < '2019-08-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-07-01')::timestamp as month from j, l);

insert into fediq_team.Arhangelsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-07-01' <= dttm and dttm < '2019-08-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-07-01' <= dttm and dttm < '2019-08-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-08-01' <= dttm and dttm < '2019-09-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-08-01')::timestamp as month from j, l);

insert into fediq_team.Arhangelsk_dolya_ottoka_201909 (with p as (select distinct(passenger_id) as passenger_id from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-08-01' <= dttm and dttm < '2019-09-01'),
    l as(select count(*) as cnt2 from p),
 t as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-08-01' <= dttm and dttm < '2019-09-01'
except
select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where status = 'complete' and '2019-09-01' <= dttm and dttm < '2019-10-01'),
j as (select count(*) as cnt1 from t)
select 1.0 * cnt1 / cnt2 as dolya_ottoka, ('2019-09-01')::timestamp as month from j, l);


create table fediq_team.Arhangelsk_rides_count_201909 as (
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_01_01' <= dttm
      and dttm <= '2018_02_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_02_01' <= dttm
      and dttm <= '2018_03_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_03_01' <= dttm
      and dttm <= '2018_04_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_04_01' <= dttm
      and dttm <= '2018_05_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_05_01' <= dttm
      and dttm <= '2018_06_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_06_01' <= dttm
      and dttm <= '2018_07_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_07_01' <= dttm
      and dttm <= '2018_08_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_08_01' <= dttm
      and dttm <= '2018_09_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_09_01' <= dttm
      and dttm <= '2018_10_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_10_01' <= dttm
      and dttm <= '2018_11_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_11_01' <= dttm
      and dttm <= '2018_12_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_12_01' <= dttm
      and dttm <= '2019_01_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_01_01' <= dttm
      and dttm <= '2019_02_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_02_01' <= dttm
      and dttm <= '2019_03_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_03_01' <= dttm
      and dttm <= '2019_04_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_04_01' <= dttm
      and dttm <= '2019_05_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_05_01' <= dttm
      and dttm <= '2019_06_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_06_01' <= dttm
      and dttm <= '2019_07_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_07_01' <= dttm
      and dttm <= '2019_08_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_08_01' <= dttm
      and dttm <= '2019_09_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id), car_class
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_09_01' <= dttm
      and dttm <= '2019_10_01'
    GROUP BY car_class
);

create table fediq_team.Arhangelsk_mean_check_201909 as (
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_01_01' <= dttm
      and dttm <= '2018_02_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_02_01' <= dttm
      and dttm <= '2018_03_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_03_01' <= dttm
      and dttm <= '2018_04_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_04_01' <= dttm
      and dttm <= '2018_05_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_05_01' <= dttm
      and dttm <= '2018_06_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_06_01' <= dttm
      and dttm <= '2018_07_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_07_01' <= dttm
      and dttm <= '2018_08_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_08_01' <= dttm
      and dttm <= '2018_09_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_09_01' <= dttm
      and dttm <= '2018_10_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_10_01' <= dttm
      and dttm <= '2018_11_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_11_01' <= dttm
      and dttm <= '2018_12_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_12_01' <= dttm
      and dttm <= '2019_01_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_01_01' <= dttm
      and dttm <= '2019_02_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_02_01' <= dttm
      and dttm <= '2019_03_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_03_01' <= dttm
      and dttm <= '2019_04_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_04_01' <= dttm
      and dttm <= '2019_05_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_05_01' <= dttm
      and dttm <= '2019_06_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_06_01' <= dttm
      and dttm <= '2019_07_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_07_01' <= dttm
      and dttm <= '2019_08_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_08_01' <= dttm
      and dttm <= '2019_09_01'
    GROUP BY car_class
    UNION
    SELECT CAST(min(dttm) AS DATE), car_class, round(sum(order_cost) / count(order_id)) as mean_check, count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_09_01' <= dttm
      and dttm <= '2019_10_01'
    GROUP BY car_class
);

create table fediq_team.Arhangelsk_offers_count_201909 as (
    SELECT CAST('2018_01_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_01_01')::timestamp
      AND (created_time)::timestamp < ('2018_02_01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
    UNION
    SELECT CAST('2018_02_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_02_01')::timestamp
      AND (created_time)::timestamp < ('2018_03_01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
    UNION
    SELECT CAST('2018_03_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_03_01')::timestamp
      AND (created_time)::timestamp < ('2018_04_01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
    UNION
    SELECT CAST('2018_04_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_04_01')::timestamp
      AND (created_time)::timestamp < ('2018_05_01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
    UNION
    SELECT CAST('2018_05_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_05_01')::timestamp
      AND (created_time)::timestamp < ('2018_06_01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
    UNION
    SELECT CAST('2018_06_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_06_01')::timestamp
      AND (created_time)::timestamp < ('2018_07_01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
    UNION
    SELECT CAST('2018_07_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_07_01')::timestamp
      AND (created_time)::timestamp < ('2018_08_01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
    UNION
    SELECT CAST('2018_08_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_08_01')::timestamp
      AND (created_time)::timestamp < ('2018_09_01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
    UNION
    SELECT CAST('2018_09_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_09_01')::timestamp
      AND (created_time)::timestamp < ('2018_10_01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
    UNION
    SELECT CAST('2018_10_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_10_01')::timestamp
      AND (created_time)::timestamp < ('2018_11_01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
    UNION
    SELECT CAST('2018_11_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_11_01')::timestamp
      AND (created_time)::timestamp < ('2018_12_01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
    UNION
    SELECT CAST('2018_12_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2018_12_01')::timestamp
      AND (created_time)::timestamp < ('2019_01_01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=59
    UNION
    SELECT CAST('2019_01_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019_01_01')::timestamp
      AND (created_time)::timestamp < ('2019_02_01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
    UNION
    SELECT CAST('2019_02_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019_02_01')::timestamp
      AND (created_time)::timestamp < ('2019_03_01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
    UNION
    SELECT CAST('2019_03_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019_03_01')::timestamp
      AND (created_time)::timestamp < ('2019_04_01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
    UNION
    SELECT CAST('2019_04_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019_04_01')::timestamp
      AND (created_time)::timestamp < ('2019_05_01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
    UNION
    SELECT CAST('2019_05_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019-05-01')::timestamp
      AND (created_time)::timestamp < ('2019-06-01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
    UNION
    SELECT CAST('2019_06_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019-06-01')::timestamp
      AND (created_time)::timestamp < ('2019-07-01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
    UNION
    SELECT CAST('2019_07_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019-07-01')::timestamp
      AND (created_time)::timestamp < ('2019-08-01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
    UNION
    SELECT CAST('2019_08_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019-08-01')::timestamp
      AND (created_time)::timestamp < ('2019-09-01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
    UNION
    SELECT CAST('2019_09_01'AS DATE), count(distinct offer_id)
    FROM fediq_team.parsed_offers_table_201909
    WHERE (created_time)::timestamp >= ('2019-09-01')::timestamp
      AND (created_time)::timestamp < ('2019-10-01')::timestamp
      AND 64<=(start_point_lat)::float8
      AND (start_point_lat)::float8<=65
);

create table fediq_team.Arhangelsk_order_count201909 as (
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_01_01' <= dttm
      and dttm <= '2018_02_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_02_01' <= dttm
      and dttm <= '2018_03_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_03_01' <= dttm
      and dttm <= '2018_04_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_04_01' <= dttm
      and dttm <= '2018_05_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_05_01' <= dttm
      and dttm <= '2018_06_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_06_01' <= dttm
      and dttm <= '2018_07_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_07_01' <= dttm
      and dttm <= '2018_08_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_08_01' <= dttm
      and dttm <= '2018_09_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_09_01' <= dttm
      and dttm <= '2018_10_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_10_01' <= dttm
      and dttm <= '2018_11_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_11_01' <= dttm
      and dttm <= '2018_12_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_12_01' <= dttm
      and dttm <= '2019_01_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2019_01_01' <= dttm
      and dttm <= '2019_02_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2019_02_01' <= dttm
      and dttm <= '2019_03_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2019_03_01' <= dttm
      and dttm <= '2019_04_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2019_04_01' <= dttm
      and dttm <= '2019_05_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2019_05_01' <= dttm
      and dttm <= '2019_06_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2019_06_01' <= dttm
      and dttm <= '2019_07_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2019_07_01' <= dttm
      and dttm <= '2019_08_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2019_08_01' <= dttm
      and dttm <= '2019_09_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(order_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2019_09_01' <= dttm
      and dttm <= '2019_10_01'
);

create table fediq_team.Arhangelsk_drivers_count_201909 as (
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_01_01' <= dttm
      and dttm <= '2018_02_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_02_01' <= dttm
      and dttm <= '2018_03_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_03_01' <= dttm
      and dttm <= '2018_04_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_04_01' <= dttm
      and dttm <= '2018_05_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_05_01' <= dttm
      and dttm <= '2018_06_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_06_01' <= dttm
      and dttm <= '2018_07_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_07_01' <= dttm
      and dttm <= '2018_08_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.nijnii_tagil_orders_201909
    WHERE '2018_08_01' <= dttm
      and dttm <= '2018_09_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_09_01' <= dttm
      and dttm <= '2018_10_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_10_01' <= dttm
      and dttm <= '2018_11_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_11_01' <= dttm
      and dttm <= '2018_12_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2018_12_01' <= dttm
      and dttm <= '2019_01_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2019_01_01' <= dttm
      and dttm <= '2019_02_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2019_02_01' <= dttm
      and dttm <= '2019_03_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2019_03_01' <= dttm
      and dttm <= '2019_04_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2019_04_01' <= dttm
      and dttm <= '2019_05_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2019_05_01' <= dttm
      and dttm <= '2019_06_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2019_06_01' <= dttm
      and dttm <= '2019_07_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2019_07_01' <= dttm
      and dttm <= '2019_08_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2019_08_01' <= dttm
      and dttm <= '2019_09_01'
    UNION
    SELECT CAST(min(dttm) AS DATE), count(DISTINCT driver_id)
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE '2019_09_01' <= dttm
      and dttm <= '2019_10_01'
);

create table fediq_team.Arhangelsk_active_2_201909 as (
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_01_01' <= dttm
            and dttm <= '2018_02_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_02_01' <= dttm
            and dttm <= '2018_03_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_03_01' <= dttm
            and dttm <= '2018_04_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_04_01' <= dttm
            and dttm <= '2018_05_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_05_01' <= dttm
            and dttm <= '2018_06_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_06_01' <= dttm
            and dttm <= '2018_07_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_07_01' <= dttm
            and dttm <= '2018_08_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_08_01' <= dttm
            and dttm <= '2018_09_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_09_01' <= dttm
            and dttm <= '2018_10_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_10_01' <= dttm
            and dttm <= '2018_11_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_11_01' <= dttm
            and dttm <= '2018_12_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_12_01' <= dttm
            and dttm <= '2019_01_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2019_01_01' <= dttm
            and dttm <= '2019_02_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2019_02_01' <= dttm
            and dttm <= '2019_03_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2019_03_01' <= dttm
            and dttm <= '2019_04_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2019_04_01' <= dttm
            and dttm <= '2019_05_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2019_05_01' <= dttm
            and dttm <= '2019_06_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2019_06_01' <= dttm
            and dttm <= '2019_07_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2019_07_01' <= dttm
            and dttm <= '2019_08_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2019_08_01' <= dttm
            and dttm <= '2019_09_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2019_09_01' <= dttm
            and dttm <= '2019_10_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 1
);

create table fediq_team.Arhangelsk_active_10_201909 as (
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_01_01' <= dttm
            and dttm <= '2018_02_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_02_01' <= dttm
            and dttm <= '2018_03_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_03_01' <= dttm
            and dttm <= '2018_04_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_04_01' <= dttm
            and dttm <= '2018_05_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_05_01' <= dttm
            and dttm <= '2018_06_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_06_01' <= dttm
            and dttm <= '2018_07_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_07_01' <= dttm
            and dttm <= '2018_08_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_08_01' <= dttm
            and dttm <= '2018_09_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_09_01' <= dttm
            and dttm <= '2018_10_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_10_01' <= dttm
            and dttm <= '2018_11_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_11_01' <= dttm
            and dttm <= '2018_12_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2018_12_01' <= dttm
            and dttm <= '2019_01_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2019_01_01' <= dttm
            and dttm <= '2019_02_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2019_02_01' <= dttm
            and dttm <= '2019_03_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2019_03_01' <= dttm
            and dttm <= '2019_04_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2019_04_01' <= dttm
            and dttm <= '2019_05_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2019_05_01' <= dttm
            and dttm <= '2019_06_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2019_06_01' <= dttm
            and dttm <= '2019_07_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2019_07_01' <= dttm
            and dttm <= '2019_08_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2019_08_01' <= dttm
            and dttm <= '2019_09_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
    UNION
    SELECT CAST(min(month) AS DATE), count(driver_id)
    FROM (SELECT min(dttm) as month, driver_id, count(order_id) as amount_of_orders
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE '2019_09_01' <= dttm
            and dttm <= '2019_10_01'
          GROUP BY driver_id) as A
    WHERE amount_of_orders > 9
);

create table fediq_team.Arhangelsk_new_drivers_201909 as (
    SELECT CAST('2018_01_01' AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2018_01_01'
            AND dttm < '2018_02_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2018_01_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_02_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2018_02_01'
            AND dttm < '2018_03_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2018_02_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_03_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2018_03_01'
            AND dttm < '2018_04_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2018_03_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_04_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2018_04_01'
            AND dttm < '2018_05_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2018_04_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_05_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2018_05_01'
            AND dttm < '2018_06_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2018_05_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_06_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2018_06_01'
            AND dttm < '2018_07_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2018_06_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_07_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2018_07_01'
            AND dttm < '2018_08_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2018_07_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_08_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2018_08_01'
            AND dttm < '2018_09_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2018_08_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_09_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2018_09_01'
            AND dttm < '2018_10_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2018_09_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_10_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2018_10_01'
            AND dttm < '2018_11_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2018_10_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_11_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2018_11_01'
            AND dttm < '2018_12_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2018_11_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2018_12_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2018_12_01'
            AND dttm < '2019_01_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2018_12_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019_01_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2019_01_01'
            AND dttm < '2019_02_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2019_01_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019_02_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2019_02_01'
            AND dttm < '2019_03_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2019_02_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019_03_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2019_03_01'
            AND dttm < '2019_04_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2019_03_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019_04_01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2019_04_01'
            AND dttm < '2019_05_01'
            AND status = 'complete' EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2019_04_01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019-05-01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2019-05-01'
            AND dttm < '2019-06-01'
            AND status = 'complete'
              EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2019-05-01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019-06-01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2019-06-01'
            AND dttm < '2019-07-01'
            AND status = 'complete'
              EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2019-06-01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019-07-01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2019-07-01'
            AND dttm < '2019-08-01'
            AND status = 'complete'
              EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2019-07-01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019-08-01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2019-08-01'
            AND dttm < '2019-09-01'
            AND status = 'complete'
              EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2019-08-01'
            AND status = 'complete') as A
    UNION
    SELECT CAST('2019-09-01'AS DATE), count(*)
    FROM (SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm >= '2019-09-01'
            AND dttm < '2019-10-01'
            AND status = 'complete'
              EXCEPT
          SELECT DISTINCT (driver_id)
          FROM fediq_team.Arhangelsk_orders_201909
          WHERE dttm < '2019-09-01'
            AND status = 'complete') as A
);

create table fediq_team.Arhangelsk_income_by_class as (
    SELECT min(dttm), round(sum(order_cost)) * 0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_01_01' <= dttm
      and dttm <= '2018_02_01'
    UNION
    SELECT min(dttm),  round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_02_01' <= dttm
      and dttm <= '2018_03_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_03_01' <= dttm
      and dttm <= '2018_04_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_04_01' <= dttm
      and dttm <= '2018_05_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_05_01' <= dttm
      and dttm <= '2018_06_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_06_01' <= dttm
      and dttm <= '2018_07_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_07_01' <= dttm
      and dttm <= '2018_08_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_08_01' <= dttm
      and dttm <= '2018_09_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_09_01' <= dttm
      and dttm <= '2018_10_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_10_01' <= dttm
      and dttm <= '2018_11_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_11_01' <= dttm
      and dttm <= '2018_12_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2018_12_01' <= dttm
      and dttm <= '2019_01_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_01_01' <= dttm
      and dttm <= '2019_02_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_02_01' <= dttm
      and dttm <= '2019_03_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_03_01' <= dttm
      and dttm <= '2019_04_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_04_01' <= dttm
      and dttm <= '2019_05_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_05_01' <= dttm
      and dttm <= '2019_06_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_06_01' <= dttm
      and dttm <= '2019_07_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_07_01' <= dttm
      and dttm <= '2019_08_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_08_01' <= dttm
      and dttm <= '2019_09_01'
    UNION
    SELECT min(dttm), round(sum(order_cost)) *0.18 as income
    FROM fediq_team.Arhangelsk_orders_201909
    WHERE status = 'complete'
      and dttm IS NOT NULL
      and '2019_09_01' <= dttm
      and dttm <= '2019_10_01'
);

ALTER TABLE fediq_team.arhangelsk_income_by_class_201909 ADD COLUMN expenses integer DEFAULT 650000;


create table fediq_team.Arhangelsk_GMV_201909 as (
    SELECT min, sum(gmv_by_car) as GMV FROM (
        SELECT min, car_class, mean_check * count as GMV_by_car FROM fediq_team.Arhangelsk_mean_check_201909
    ) AS T
    GROUP BY min
);

create table fediq_team.Arhangelsk_amount_treap_201909 as(
    SELECT min, sum(count)
    FROM fediq_team.Arhangelsk_mean_check_201909
    GROUP BY min
);

create table fediq_team.Arhangelsk_new_client_201909 as(
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2018-01-01' and dttm < '2018-02-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2018-01-01'and status = 'complete')
    select count(*), ('2018-01-01')::timestamp
    from p
);

insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2018-02-01' and dttm < '2018-03-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2018-02-01'and status = 'complete')
    select count(*), ('2018-02-01')::timestamp
    from p
);

insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2018-03-01' and dttm < '2018-04-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2018-03-01'and status = 'complete')
    select count(*), ('2018-03-01')::timestamp
    from p
);
insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2018-04-01' and dttm < '2018-05-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2018-04-01'and status = 'complete')
    select count(*), ('2018-04-01')::timestamp
    from p
);
insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2018-05-01' and dttm < '2018-06-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2018-05-01'and status = 'complete')
    select count(*), ('2018-05-01')::timestamp
    from p
);
insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2018-06-01' and dttm < '2018-07-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2018-06-01'and status = 'complete')
    select count(*), ('2018-06-01')::timestamp
    from p
);
insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2018-07-01' and dttm < '2018-08-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2018-07-01'and status = 'complete')
    select count(*), ('2018-07-01')::timestamp
    from p
);
insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2018-08-01' and dttm < '2018-09-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2018-08-01'and status = 'complete')
    select count(*), ('2018-08-01')::timestamp
    from p
);
insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2018-09-01' and dttm < '2018-10-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2018-09-01'and status = 'complete')
    select count(*), ('2018-09-01')::timestamp
    from p
);
insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2018-10-01' and dttm < '2018-11-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2018-10-01'and status = 'complete')
    select count(*), ('2018-10-01')::timestamp
    from p
);
insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2018-11-01' and dttm < '2018-12-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2018-11-01'and status = 'complete')
    select count(*), ('2018-11-01')::timestamp
    from p
);
insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2018-12-01' and dttm < '2019-01-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2018-12-01'and status = 'complete')
    select count(*), ('2018-12-01')::timestamp
    from p
);
insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2019-01-01' and dttm < '2019-02-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2019-01-01'and status = 'complete')
    select count(*), ('2019-01-01')::timestamp
    from p
);
insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2019-02-01' and dttm < '2019-03-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2019-02-01'and status = 'complete')
    select count(*), ('2019-02-01')::timestamp
    from p
);
insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2019-03-01' and dttm < '2019-04-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2019-03-01'and status = 'complete')
    select count(*), ('2019-03-01')::timestamp
    from p
);
insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2019-04-01' and dttm < '2019-05-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2019-04-01'and status = 'complete')
    select count(*), ('2019-04-01')::timestamp
    from p
);
insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2019-05-01' and dttm < '2019-06-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2019-05-01'and status = 'complete')
    select count(*), ('2019-05-01')::timestamp
    from p
);
insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2019-06-01' and dttm < '2019-07-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2019-06-01'and status = 'complete')
    select count(*), ('2019-06-01')::timestamp
    from p
);
insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2019-07-01' and dttm < '2019-08-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2019-07-01'and status = 'complete')
    select count(*), ('2019-07-01')::timestamp
    from p
);
insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2019-08-01' and dttm < '2019-09-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2019-08-01'and status = 'complete')
    select count(*), ('2019-08-01')::timestamp
    from p
);
insert into fediq_team.Arhangelsk_new_client_201909 (
    with p as (select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm >= '2019-09-01' and dttm < '2019-10-01' and status = 'complete'
    except
    select distinct(passenger_id) from fediq_team.Arhangelsk_orders_201909
    where dttm < '2019-09-01'and status = 'complete')
    select count(*), ('2019-09-01')::timestamp
    from p
);

create table fediq_team.Arhangelsk_kernel_201909 as (
    with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.Arhangelsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-02-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-01-01' and cnt >= 4)
    select count(*), ('2018-02-01')::timestamp
from t
);
insert into fediq_team.Arhangelsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.Arhangelsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-03-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-02-01' and cnt >= 4)
    select count(*), ('2018-03-01')::timestamp
from t
);
insert into fediq_team.Arhangelsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.Arhangelsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-04-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-03-01' and cnt >= 4)
    select count(*), ('2018-04-01')::timestamp
from t
);
insert into fediq_team.Arhangelsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.Arhangelsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-05-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-04-01' and cnt >= 4)
    select count(*), ('2018-05-01')::timestamp
from t
);
insert into fediq_team.Arhangelsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.Arhangelsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-06-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-05-01' and cnt >= 4)
    select count(*), ('2018-06-01')::timestamp
from t
);
insert into fediq_team.Arhangelsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.Arhangelsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-07-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-06-01' and cnt >= 4)
    select count(*), ('2018-07-01')::timestamp
from t
);
insert into fediq_team.Arhangelsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.Arhangelsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-08-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-07-01' and cnt >= 4)
    select count(*), ('2018-08-01')::timestamp
from t
);
insert into fediq_team.Arhangelsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.Arhangelsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-09-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-08-01' and cnt >= 4)
    select count(*), ('2018-09-01')::timestamp
from t
);
insert into fediq_team.Arhangelsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.Arhangelsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-10-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-09-01' and cnt >= 4)
    select count(*), ('2018-10-01')::timestamp
from t
);
insert into fediq_team.Arhangelsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.Arhangelsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-11-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-10-01' and cnt >= 4)
    select count(*), ('2018-11-01')::timestamp
from t
);
insert into fediq_team.Arhangelsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.Arhangelsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2018-12-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-11-01' and cnt >= 4)
    select count(*), ('2018-12-01')::timestamp
from t
);
insert into fediq_team.Arhangelsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.Arhangelsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-01-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2018-12-01' and cnt >= 4)
    select count(*), ('2019-01-01')::timestamp
from t
);
insert into fediq_team.Arhangelsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.Arhangelsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-02-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-01-01' and cnt >= 4)
    select count(*), ('2019-02-01')::timestamp
from t
);
insert into fediq_team.Arhangelsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.nijnii_tagil_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-03-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-02-01' and cnt >= 4)
    select count(*), ('2019-03-01')::timestamp
from t
);
insert into fediq_team.Arhangelsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.Arhangelsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-04-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-03-01' and cnt >= 4)
    select count(*), ('2019-04-01')::timestamp
from t
);
insert into fediq_team.Arhangelsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.Arhangelsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-05-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-04-01' and cnt >= 4)
    select count(*), ('2019-05-01')::timestamp
from t
);
insert into fediq_team.Arhangelsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.Arhangelsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-06-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-05-01' and cnt >= 4)
    select count(*), ('2019-06-01')::timestamp
from t
);
insert into fediq_team.Arhangelsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.Arhangelsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-07-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-06-01' and cnt >= 4)
    select count(*), ('2019-07-01')::timestamp
from t
);
insert into fediq_team.Arhangelsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.Arhangelsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-08-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-07-01' and cnt >= 4)
    select count(*), ('2019-08-01')::timestamp
from t
);
insert into fediq_team.Arhangelsk_kernel_201909 (
         with p as (select passenger_id, date_trunc('month', dttm) as month, count(*) as cnt from fediq_team.Arhangelsk_orders_201909 where status='complete' group by month, passenger_id),
 t as (select  passenger_id from p where month = '2019-09-01' and cnt >= 4
     intersect
     select  passenger_id from p where month = '2019-08-01' and cnt >= 4)
    select count(*), ('2019-09-01')::timestamp
from t
);

create table fediq_team.Arhangelsk_old_client_201909 as(
    SELECT month, mau - count as old_pay
    FROM fediq_team.Arhangelsk_mau_201909
    INNER JOIN fediq_team.Arhangelsk_new_client_201909 ON month = timestamp
);



grant all privileges on table fediq_team.orders_table_201909 to idmashaantonenko ,m8element
,yaroslawserow
,sad2017a
with grant option;
grant all privileges on table fediq_team.offers_table_201909 to idmashaantonenko ,m8element
,yaroslawserow
,sad2017a
with grant option;
grant all privileges on table fediq_team.parsed_offers_table_201909 to idmashaantonenko ,m8element
,yaroslawserow
,sad2017a
with grant option;
grant all privileges on table fediq_team.parsed_orders_table_201909 to idmashaantonenko ,m8element
,yaroslawserow
,sad2017a
with grant option;
grant all privileges on table fediq_team.novochahtinsk_active_10_201909,
fediq_team.novochahtinsk_active_2_201909,
fediq_team.novochahtinsk_dolya_ottoka_201909,
fediq_team.novochahtinsk_drivers_count_201909,
fediq_team.novochahtinsk_gmv_201909,
fediq_team.novochahtinsk_income_by_class_201909,
fediq_team.novochahtinsk_kernel_201909,
fediq_team.novochahtinsk_mau_201909,
fediq_team.novochahtinsk_mean_check_201909,
fediq_team.novochahtinsk_new_client_201909,
fediq_team.novochahtinsk_new_drivers_201909,
fediq_team.novochahtinsk_offers_count_201909,
fediq_team.novochahtinsk_old_client_201909,
fediq_team.novochahtinsk_order_count201909,
fediq_team.novochahtinsk_orders_201909,
fediq_team.novochahtinsk_rides_count_201909,
fediq_team.novoshakhtinsk_dau_201909,
fediq_team.novoshakhtinsk_rettention_2019_09,
fediq_team.novochantinsk_amount_treap_201909 to idmashaantonenko ,m8element
,yaroslawserow
,sad2017a
with grant option;
grant all privileges on table
fediq_team.nijnii_tagil_active_10_201909,
fediq_team.nijnii_tagil_active_2_201909,
fediq_team.nijnii_tagil_amount_treap_201909,
fediq_team.nijnii_tagil_dau_201909,
fediq_team.nijnii_tagil_dolya_ottoka_201909,
fediq_team.nijnii_tagil_drivers_count_201909,
fediq_team.nijnii_tagil_gmv_201909,
fediq_team.nijnii_tagil_income_by_class,
fediq_team.nijnii_tagil_kernel_201909,
fediq_team.nijnii_tagil_mau_201909,
fediq_team.nijnii_tagil_mean_check_201909,
fediq_team.nijnii_tagil_new_client_201909,
fediq_team.nijnii_tagil_new_drivers_201909,
fediq_team.nijnii_tagil_offers_count_201909,
fediq_team.nijnii_tagil_old_client_201909,
fediq_team.nijnii_tagil_order_count201909,
fediq_team.nijnii_tagil_orders_201909,
fediq_team.nijnii_tagil_rettention_2019_09,
fediq_team.nijnii_tagil_rides_count_201909
to idmashaantonenko ,m8element
,yaroslawserow
,sad2017a
with grant option;

grant all privileges on table
fediq_team.arhangelsk_active_10_201909,
fediq_team.arhangelsk_active_2_201909,
fediq_team.arhangelsk_amount_treap_201909,
fediq_team.arhangelsk_dau_201909,
fediq_team.arhangelsk_dolya_ottoka_201909,
fediq_team.arhangelsk_drivers_count_201909,
fediq_team.arhangelsk_gmv_201909,
fediq_team.arhangelsk_income_by_class_201909,
fediq_team.arhangelsk_kernel_201909,
fediq_team.arhangelsk_mau_201909,
fediq_team.arhangelsk_mean_check_201909,
fediq_team.arhangelsk_new_client_201909,
fediq_team.arhangelsk_new_drivers_201909,
fediq_team.arhangelsk_offers_count_201909,
fediq_team.arhangelsk_old_client_201909,
fediq_team.arhangelsk_order_count201909,
fediq_team.arhangelsk_orders_201909,
fediq_team.arhangelsk_rides_count_201909
to idmashaantonenko ,m8element
,yaroslawserow
,sad2017a
with grant option;

grant all privileges on table fediq_team.pins_parsed_table_201909 to idmashaantonenko ,m8element
,yaroslawserow
,sad2017a
with grant option;
grant all privileges on table fediq_team.users_table_201909 to idmashaantonenko ,m8element
,yaroslawserow
,sad2017a
with grant option;
grant all privileges on table fediq_team.unique_offers to idmashaantonenko ,m8element
,yaroslawserow
,sad2017a
with grant option;
