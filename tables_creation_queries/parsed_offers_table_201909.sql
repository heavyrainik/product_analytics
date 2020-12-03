create table fediq_team.parsed_offers_table_201909 as
with offer as(
select
    doc::json as dj
from common.offers_fediq
),
parsed_offer as (
    select
        (dj ->> '_id') as offer_id,
        (dj ->> 'user_id') as user_id,
        (dj ->> 'created') as created_time,
        (dj ->> 'time') as expected_time_in_trip_offer,
        (dj -> 'price_modifiers' -> 'items' -> 0 ->> 'reason') as ya_plus_subscriber,
        (dj-> 'route' -> 0 ->> 1) as start_point_lat

    from offer
)
select *
from parsed_offer;
