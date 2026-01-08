select
  order_id,
  order_ts,
  customer_id,
  sku,
  quantity,
  price_amount as unit_price,
  (price_amount * quantity) as order_total,
  status,
  inserted_at as event_inserted_at
from {{ ref('stg_order_created') }}
