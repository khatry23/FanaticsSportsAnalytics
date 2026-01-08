with raw as (
  select
    event_id,
    source,
    event_type,
    payload,
    inserted_at
  from {{ source('raw', 'events') }}
  where event_type = 'ManufacturingBatch'
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
  payload:batch_id::string as batch_id,
  payload:sku::string as sku,
  payload:production_date::date as production_date,
  payload:quantity_produced::number as quantity_produced,
  payload:defect_rate_estimate:units::number as defect_rate_units,
  payload:defect_rate_estimate:scale::number as defect_rate_scale,
  {{ decimal_to_numeric('payload:defect_rate_estimate:units::number', 'payload:defect_rate_estimate:scale::number') }} as defect_rate_estimate
from deduped
where rn = 1
