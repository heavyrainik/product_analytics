create table fediq_team.nijnii_tagil_amount_treap_201909 as(
    SELECT min, sum(count)
    FROM fediq_team.novochahtinsk_mean_check_201909
    GROUP BY min
);
