create table fediq_team.Nijnii_tagil_orders_201909 as(
    SELECT *
    FROM fediq_team.orders_table_201909
    WHERE 57<=start_point_a_lat and start_point_a_lat <= 59
);
