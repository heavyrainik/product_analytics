create table fediq_team.novochahtinsk_GMV_201909 as (
    SELECT min, sum(gmv_by_car) as GMV FROM (
        SELECT min, car_class, mean_check * count as GMV_by_car FROM fediq_team.novochahtinsk_mean_check_201909
    ) AS T
    GROUP BY min
);
