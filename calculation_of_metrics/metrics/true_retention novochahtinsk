create table fediq_team.rettenrion_novochahtinsk_201909 as(
    with p as(
        select *
        from (select mau - count as users_end, date_trunc('month', timestamp - INTERVAL '365' DAY TO SECOND) as dttm
        from fediq_team.novochahtinsk_mau_201909
        inner join fediq_team.novochahtinsk_new_client_201909 on timestamp = month) as T
        inner join fediq_team.novochahtinsk_mau_201909 on T.dttm = month
        )
    select (users_end)::float / (mau)::float as rettention, dttm
    from p
);
