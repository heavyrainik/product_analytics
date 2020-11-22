create table fediq_team.novochahtinsk_orders_201909 as(
    SELECT *
    FROM fediq_team.orders_table_201909
    WHERE 47<=start_point_a_lat and start_point_a_lat <= 48
)
