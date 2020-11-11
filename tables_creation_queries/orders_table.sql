create table fediq_team.order_tables_201801 as(
    SELECT *
    FROM fediq_team.order_tables
    WHERE (dttm >= '2018-01-01 00:00:00.000000' AND dttm <= '2018-01-31 23:59:59.999999')
);
create table fediq_team.order_tables_201802 as(
    SELECT *
    FROM fediq_team.order_tables
    WHERE (dttm >= '2018-02-01 00:00:00.000000' AND dttm <= '2018-02-28 23:59:59.999999')
);
create table fediq_team.order_tables_201803 as(
    SELECT *
    FROM fediq_team.order_tables
    WHERE (dttm >= '2018-03-01 00:00:00.000000' AND dttm <= '2018-03-31 23:59:59.999999')
);
create table fediq_team.order_tables_201804 as(
    SELECT *
    FROM fediq_team.order_tables
    WHERE (dttm >= '2018-04-01 00:00:00.000000' AND dttm <= '2018-04-30 23:59:59.999999')
);
create table fediq_team.order_tables_201805 as(
    SELECT *
    FROM fediq_team.order_tables
    WHERE (dttm >= '2018-05-01 00:00:00.000000' AND dttm <= '2018-05-31 23:59:59.999999')
);
create table fediq_team.order_tables_201806 as(
    SELECT *
    FROM fediq_team.order_tables
    WHERE (dttm >= '2018-06-01 00:00:00.000000' AND dttm <= '2018-06-30 23:59:59.999999')
);
create table fediq_team.order_tables_201807 as(
    SELECT *
    FROM fediq_team.order_tables
    WHERE (dttm >= '2018-07-01 00:00:00.000000' AND dttm <= '2018-07-31 23:59:59.999999')
);
create table fediq_team.order_tables_201808 as(
    SELECT *
    FROM fediq_team.order_tables
    WHERE (dttm >= '2018-08-01 00:00:00.000000' AND dttm <= '2018-08-31 23:59:59.999999')
);
create table fediq_team.order_tables_201809 as(
    SELECT *
    FROM fediq_team.order_tables
    WHERE (dttm >= '2018-09-01 00:00:00.000000' AND dttm <= '2018-09-30 23:59:59.999999')
);
create table fediq_team.order_tables_201810 as(
    SELECT *
    FROM fediq_team.order_tables
    WHERE (dttm >= '2018-10-01 00:00:00.000000' AND dttm <= '2018-10-31 23:59:59.999999')
);
create table fediq_team.order_tables_201811 as(
    SELECT *
    FROM fediq_team.order_tables
    WHERE (dttm >= '2018-11-01 00:00:00.000000' AND dttm <= '2018-11-30 23:59:59.999999')
);
create table fediq_team.order_tables_201812 as(
    SELECT *
    FROM fediq_team.order_tables
    WHERE (dttm >= '2018-12-01 00:00:00.000000' AND dttm <= '2018-12-30 23:59:59.999999')
);
create table fediq_team.order_tables_201901 as(
    SELECT *
    FROM fediq_team.order_tables
    WHERE (dttm >= '2019-01-01 00:00:00.000000' AND dttm <= '2019-01-31 23:59:59.999999')
);
create table fediq_team.order_tables_201902 as(
    SELECT *
    FROM fediq_team.order_tables
    WHERE (dttm >= '2019-02-01 00:00:00.000000' AND dttm <= '2019-02-28 23:59:59.999999')
);
create table fediq_team.order_tables_201903 as(
    SELECT *
    FROM fediq_team.order_tables
    WHERE (dttm >= '2019-03-01 00:00:00.000000' AND dttm <= '2019-03-31 23:59:59.999999')
);
create table fediq_team.order_tables_201904 as(
    SELECT *
    FROM fediq_team.order_tables
    WHERE (dttm >= '2019-04-01 00:00:00.000000' AND dttm <= '2019-04-30 23:59:59.999999')
);
create table fediq_team.order_tables_201905 as(
    SELECT *
    FROM fediq_team.order_tables
    WHERE (dttm >= '2019-05-01 00:00:00.000000' AND dttm <= '2019-05-31 23:59:59.999999')
);

create table fediq_team.order_tables as
(
    SELECT o.*, p.created_time, p.pin_id, p.estimated_waiting
    from fediq_team.parsed_orders_table as o
    LEFT JOIN fediq_team.pins_parsed_table as p ON o.order_id = p.order_id
);
