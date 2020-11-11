create table fediq_team.offer_tables as
(
    SELECT df.offer_id, df.start_ofer_session, df.finish_offer_session, user_id
    FROM (SELECT offer.offer_id, min(p.created_time) as start_ofer_session, max(p.created_time) as finish_offer_session
    from fediq_team.parsed_offers_table as offer
    RIGHT JOIN fediq_team.pins_parsed_table as p ON offer.offer_id = p.offer_id
    GROUP BY offer.offer_id) as df
    INNER JOIN fediq_team.parsed_offers_table ON fediq_team.parsed_offers_table .offer_id = df.offer_id
);
create table fediq_team.offer_table_201801 as(
    SELECT *
    FROM fediq_team.offer_tables
    WHERE (start_ofer_session >= '2018-01-01 00:00:00.000000' AND start_ofer_session <= '2018-01-31 23:59:59.999999')
);
create table fediq_team.offer_table_201802 as(
    SELECT *
    FROM fediq_team.offer_tables
    WHERE (start_ofer_session >= '2018-02-01 00:00:00.000000' AND start_ofer_session <= '2018-02-28 23:59:59.999999')
);
create table fediq_team.offer_table_201803 as(
    SELECT *
    FROM fediq_team.offer_tables
    WHERE (start_ofer_session >= '2018-03-01 00:00:00.000000' AND start_ofer_session <= '2018-03-31 23:59:59.999999')
);
create table fediq_team.offer_table_201804 as(
    SELECT *
    FROM fediq_team.offer_tables
    WHERE (start_ofer_session >= '2018-04-01 00:00:00.000000' AND start_ofer_session <= '2018-04-30 23:59:59.999999')
);
create table fediq_team.offer_table_201805 as(
    SELECT *
    FROM fediq_team.offer_tables
    WHERE (start_ofer_session >= '2018-05-01 00:00:00.000000' AND start_ofer_session <= '2018-05-31 23:59:59.999999')
);
create table fediq_team.offer_table_201806 as(
    SELECT *
    FROM fediq_team.offer_tables
    WHERE (start_ofer_session >= '2018-06-01 00:00:00.000000' AND start_ofer_session <= '2018-06-30 23:59:59.999999')
);
create table fediq_team.offer_table_201807 as(
    SELECT *
    FROM fediq_team.offer_tables
    WHERE (start_ofer_session >= '2018-07-01 00:00:00.000000' AND start_ofer_session <= '2018-07-31 23:59:59.999999')
);
create table fediq_team.offer_table_201808 as(
    SELECT *
    FROM fediq_team.offer_tables
    WHERE (start_ofer_session >= '2018-08-01 00:00:00.000000' AND start_ofer_session <= '2018-08-31 23:59:59.999999')
);
create table fediq_team.offer_table_201809 as(
    SELECT *
    FROM fediq_team.offer_tables
    WHERE (start_ofer_session >= '2018-09-01 00:00:00.000000' AND start_ofer_session <= '2018-09-30 23:59:59.999999')
);
create table fediq_team.offer_table_201810 as(
    SELECT *
    FROM fediq_team.offer_tables
    WHERE (start_ofer_session >= '2018-10-01 00:00:00.000000' AND start_ofer_session <= '2018-10-31 23:59:59.999999')
);
create table fediq_team.offer_table_201811 as(
    SELECT *
    FROM fediq_team.offer_tables
    WHERE (start_ofer_session >= '2018-11-01 00:00:00.000000' AND start_ofer_session <= '2018-11-30 23:59:59.999999')
);
create table fediq_team.offer_table_201812 as(
    SELECT *
    FROM fediq_team.offer_tables
    WHERE (start_ofer_session >= '2018-12-01 00:00:00.000000' AND start_ofer_session <= '2018-12-31 23:59:59.999999')
);
create table fediq_team.offer_table_201901 as(
    SELECT *
    FROM fediq_team.offer_tables
    WHERE (start_ofer_session >= '2019-01-01 00:00:00.000000' AND start_ofer_session <= '2019-01-31 23:59:59.999999')
);
create table fediq_team.offer_table_201902 as(
    SELECT *
    FROM fediq_team.offer_tables
    WHERE (start_ofer_session >= '2019-02-01 00:00:00.000000' AND start_ofer_session <= '2019-02-28 23:59:59.999999')
);
create table fediq_team.offer_table_201903 as(
    SELECT *
    FROM fediq_team.offer_tables
    WHERE (start_ofer_session >= '2019-03-01 00:00:00.000000' AND start_ofer_session <= '2019-03-31 23:59:59.999999')
);
create table fediq_team.offer_table_201904 as(
    SELECT *
    FROM fediq_team.offer_tables
    WHERE (start_ofer_session >= '2019-04-01 00:00:00.000000' AND start_ofer_session <= '2019-04-30 23:59:59.999999')
);
create table fediq_team.offer_table_201905 as(
    SELECT *
    FROM fediq_team.offer_tables
    WHERE (start_ofer_session >= '2019-05-01 00:00:00.000000' AND start_ofer_session <= '2019-05-31 23:59:59.999999')
);