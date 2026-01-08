with ranked as (
  select
    sku,
    card_name,
    sport,
    player_name,
    team_name,
    parallel,
    print_run,
    inserted_at,
    row_number() over (partition by sku order by inserted_at desc) as rn
  from {{ ref('stg_card_catalog_entry_inserted') }}
)

select
  sku,
  card_name,
  sport,
  player_name,
  team_name,
  parallel,
  print_run,
  inserted_at as last_updated_at
from ranked
where rn = 1
