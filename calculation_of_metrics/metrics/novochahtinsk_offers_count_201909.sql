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
