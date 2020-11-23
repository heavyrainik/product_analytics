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
