create table fediq_team.users as
(
    SELECT user_id, start_time, ya_plus_subscriber
    FROM fediq_team.parsed_offers_table_with_subscriber
    JOIN (SELECT fediq_team.parsed_orders_table.passenger_id, min (fediq_team.parsed_orders_table.dttm) as start_time
    FROM fediq_team.parsed_orders_table
    GROUP BY passenger_id) as A ON fediq_team.parsed_offers_table_with_subscriber.user_id = A.passenger_id
);
--делим юзеров на куски
create table fediq_team.users_201801 as(
    SELECT *
    FROM fediq_team.users
    WHERE (start_time >= '2018-01-01 00:00:00.000000' AND start_time <= '2018-01-31 23:59:59.999999')
);
create table fediq_team.users_201802 as(
    SELECT *
    FROM fediq_team.users
    WHERE (start_time >= '2018-02-01 00:00:00.000000' AND start_time <= '2018-02-28 23:59:59.999999')
);
create table fediq_team.users_201803 as(
    SELECT *
    FROM fediq_team.users
    WHERE (start_time >= '2018-03-01 00:00:00.000000' AND start_time <= '2018-03-31 23:59:59.999999')
);
create table fediq_team.users_201804 as(
    SELECT *
    FROM fediq_team.users
    WHERE (start_time >= '2018-04-01 00:00:00.000000' AND start_time <= '2018-04-30 23:59:59.999999')
);
create table fediq_team.users_201805 as(
    SELECT *
    FROM fediq_team.users
    WHERE (start_time >= '2018-05-01 00:00:00.000000' AND start_time <= '2018-05-31 23:59:59.999999')
);
create table fediq_team.users_201806 as(
    SELECT *
    FROM fediq_team.users
    WHERE (start_time >= '2018-06-01 00:00:00.000000' AND start_time <= '2018-06-30 23:59:59.999999')
);
create table fediq_team.users_201807 as(
    SELECT *
    FROM fediq_team.users
    WHERE (start_time >= '2018-07-01 00:00:00.000000' AND start_time <= '2018-07-31 23:59:59.999999')
);
create table fediq_team.users_201808 as(
    SELECT *
    FROM fediq_team.users
    WHERE (start_time >= '2018-08-01 00:00:00.000000' AND start_time <= '2018-08-31 23:59:59.999999')
);
create table fediq_team.users_201809 as(
    SELECT *
    FROM fediq_team.users
    WHERE (start_time >= '2018-09-01 00:00:00.000000' AND start_time <= '2018-09-30 23:59:59.999999')
);
create table fediq_team.users_201810 as(
    SELECT *
    FROM fediq_team.users
    WHERE (start_time >= '2018-10-01 00:00:00.000000' AND start_time <= '2018-10-31 23:59:59.999999')
);
create table fediq_team.users_201811 as(
    SELECT *
    FROM fediq_team.users
    WHERE (start_time >= '2018-11-01 00:00:00.000000' AND start_time <= '2018-11-30 23:59:59.999999')
);
create table fediq_team.users_201812 as(
    SELECT *
    FROM fediq_team.users
    WHERE (start_time >= '2018-12-01 00:00:00.000000' AND start_time <= '2018-12-31 23:59:59.999999')
);
create table fediq_team.users_201901 as(
    SELECT *
    FROM fediq_team.users
    WHERE (start_time >= '2019-01-01 00:00:00.000000' AND start_time <= '2019-01-31 23:59:59.999999')
);
create table fediq_team.users_201902 as(
    SELECT *
    FROM fediq_team.users
    WHERE (start_time >= '2019-02-01 00:00:00.000000' AND start_time <= '2019-02-28 23:59:59.999999')
);
create table fediq_team.users_201903 as(
    SELECT *
    FROM fediq_team.users
    WHERE (start_time >= '2019-03-01 00:00:00.000000' AND start_time <= '2019-03-31 23:59:59.999999')
);
create table fediq_team.users_201904 as(
    SELECT *
    FROM fediq_team.users
    WHERE (start_time >= '2019-04-01 00:00:00.000000' AND start_time <= '2019-04-30 23:59:59.999999')
);
create table fediq_team.users_201905 as(
    SELECT *
    FROM fediq_team.users
    WHERE (start_time >= '2019-05-01 00:00:00.000000' AND start_time <= '2019-05-31 23:59:59.999999')
);