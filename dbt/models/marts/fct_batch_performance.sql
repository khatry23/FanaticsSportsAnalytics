select
  batch_id,
  sku,
  production_date,
  quantity_produced,
  defect_rate_estimate,
  (quantity_produced * defect_rate_estimate) as estimated_defects,
  (quantity_produced - (quantity_produced * defect_rate_estimate)) as estimated_good_units,
  inserted_at as event_inserted_at
from {{ ref('stg_manufacturing_batch') }}
