create table fediq_team.{city}_GMV_201909 as (
    SELECT week, sum(gmv_by_class) as GMV FROM (
        SELECT week, mean_check*count as GMV_by_class
        FROM fediq_team.{city}_mean_check201909
    ) AS A
    GROUP BY week
);
