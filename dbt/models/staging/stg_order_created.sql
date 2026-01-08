with raw as (
  select
    event_id,
    source,
    event_type,
    payload,
    inserted_at
  from {{ source('raw', 'events') }}
  where event_type = 'OrderCreated'
),

deduped as (
  select
    event_id,
    source,
    event_type,
    payload,
    inserted_at,
    row_number() over (partition by event_id order by inserted_at desc) as rn
  from raw
)

select
  event_id,
  source,
  event_type,
  inserted_at,
  payload:order_id::string as order_id,
  payload:order_ts::timestamp_tz as order_ts,
  payload:customer_id::string as customer_id,
  payload:sku::string as sku,
  payload:quantity::number as quantity,
  payload:price:units::number as price_units,
  payload:price:scale::number as price_scale,
  {{ decimal_to_numeric('payload:price:units::number', 'payload:price:scale::number') }} as price_amount,
  payload:status::string as status
from deduped
where rn = 1
