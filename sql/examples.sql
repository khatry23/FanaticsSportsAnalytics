-- Example analytics queries

-- Top 10 SKUs by revenue
select
  sku,
  sum(order_total) as total_revenue,
  sum(quantity) as total_units
from SPORTS_CARD_ANALYTICS.GOLD.FCT_ORDERS
group by 1
order by total_revenue desc
limit 10;

-- Defect rate summary by SKU
select
  sku,
  avg(defect_rate_estimate) as avg_defect_rate,
  sum(quantity_produced) as total_produced
from SPORTS_CARD_ANALYTICS.GOLD.FCT_BATCH_PERFORMANCE
group by 1
order by avg_defect_rate desc;

-- Latest card catalog entries
select
  sku,
  card_name,
  sport,
  player_name,
  team_name,
  parallel,
  print_run
from SPORTS_CARD_ANALYTICS.GOLD.DIM_CARD
order by sku;
