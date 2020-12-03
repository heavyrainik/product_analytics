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
