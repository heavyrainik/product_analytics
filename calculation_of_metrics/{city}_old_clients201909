create table fediq_team.{city}_old_clients201909 as(
    SELECT week, wau - count as old_clients
    FROM fediq_team.{city}_wau_201909
    INNER JOIN fediq_team.{city}_new_clients201909 ON week = cohort_week
);
