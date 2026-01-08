with raw as (
  select
    event_id,
    source,
    event_type,
    payload,
    inserted_at
  from {{ source('raw', 'events') }}
  where event_type = 'CardCatalogEntryInserted'
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
  payload:sku::string as sku,
  payload:card_name::string as card_name,
  payload:sport::string as sport,
  payload:player_name::string as player_name,
  payload:team_name::string as team_name,
  payload:parallel::string as parallel,
  payload:print_run::number as print_run
from deduped
where rn = 1
