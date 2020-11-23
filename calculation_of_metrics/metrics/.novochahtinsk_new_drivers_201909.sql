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
