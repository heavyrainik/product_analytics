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
