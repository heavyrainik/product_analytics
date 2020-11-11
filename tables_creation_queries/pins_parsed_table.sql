create table fediq_team.pins_parsed_table as
with pin as(
select
    doc::json as dj
from common.pinstats_fediq
),
parsed_pin as (
    select
        (dj ->> '_id') as pin_id,
        (dj ->> 'offer_id') as offer_id,
        (dj ->> 'order_id') as order_id,
        (dj ->> 'user_id') as user_id,
        (dj ->> 'created')::timestamp as created_time,
        (dj ->> 'estimated_waiting') as estimated_waiting

    from pin
)
select *
from parsed_pin;