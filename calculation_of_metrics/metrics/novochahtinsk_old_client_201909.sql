create table fediq_team.novochahtinsk_old_client_201909 as(
    SELECT month, mau - count as old_pay
    FROM fediq_team.novochahtinsk_mau_201909
    INNER JOIN fediq_team.novochahtinsk_new_client_201909 ON month = timestamp
);
