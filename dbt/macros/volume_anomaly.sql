{% test volume_anomaly(model, timestamp_column, threshold_pct=0.5, lookback_days=7) %}
  with daily_counts as (
    select
      date_trunc('day', {{ timestamp_column }}) as event_date,
      count(*) as event_count
    from {{ model }}
    where {{ timestamp_column }} is not null
    group by 1
  ),
  with_avg as (
    select
      event_date,
      event_count,
      avg(event_count) over (
        order by event_date
        rows between {{ lookback_days }} preceding and 1 preceding
      ) as rolling_avg
    from daily_counts
  )
  select *
  from with_avg
  where rolling_avg is not null
    and rolling_avg > 0
    and abs(event_count - rolling_avg) / rolling_avg > {{ threshold_pct }}
{% endtest %}
