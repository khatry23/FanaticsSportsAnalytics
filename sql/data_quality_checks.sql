-- Basic data quality checks for Gold models

-- Ensure no negative quantities
select count(*) as negative_order_qty
from SPORTS_CARD_ANALYTICS.GOLD.FCT_ORDERS
where quantity < 0;

-- Ensure defect rate within [0, 1]
select count(*) as invalid_defect_rate
from SPORTS_CARD_ANALYTICS.GOLD.FCT_BATCH_PERFORMANCE
where defect_rate_estimate < 0 or defect_rate_estimate > 1;

-- Ensure every order SKU exists in dim_card
select count(*) as missing_dim_card
from SPORTS_CARD_ANALYTICS.GOLD.FCT_ORDERS o
left join SPORTS_CARD_ANALYTICS.GOLD.DIM_CARD d
  on o.sku = d.sku
where d.sku is null;

-- Freshness check (replace threshold as needed)
select
  datediff('hour', max(inserted_at), current_timestamp()) as hours_since_latest_event
from SPORTS_CARD_ANALYTICS.RAW.EVENTS;

-- Freshness violation rows (configured by env vars in Airflow)
select
  max(inserted_at) as latest_event_ts
from SPORTS_CARD_ANALYTICS.RAW.EVENTS
having datediff('hour', max(inserted_at), current_timestamp()) > __FRESHNESS_ERROR_HOURS__;

-- Volume anomaly check vs rolling 7-day average (configured by env vars in Airflow)
with daily_counts as (
  select
    date_trunc('day', inserted_at) as event_date,
    count(*) as event_count
  from SPORTS_CARD_ANALYTICS.RAW.EVENTS
  group by 1
),
with_avg as (
  select
    event_date,
    event_count,
    avg(event_count) over (
      order by event_date
      rows between __VOLUME_LOOKBACK_DAYS__ preceding and 1 preceding
    ) as rolling_avg
  from daily_counts
)
select *
from with_avg
where rolling_avg is not null
  and rolling_avg > 0
  and abs(event_count - rolling_avg) / rolling_avg > __VOLUME_ANOMALY_PCT__;
