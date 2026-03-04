with base as (
    select
        location_id,
        dates,
        temperature_2m_max,
        temperature_2m_min,
        temperature_2m_mean,
        precipitation_sum,
        rain_sum,
        snowfall_sum,
        sunshine_duration,
        wind_speed_10m_max,
        precip_hours
    from {{ ref('fct_weather_daily') }}
),
recon as (
    select
        location_id,
        dates,
        abs(diff_temp_max_api_vs_hourly) as abs_diff_temp_max,
        abs(diff_temp_min_api_vs_hourly) as abs_diff_temp_min,
        abs(diff_precip_api_vs_hourly) as abs_diff_precip
    from {{ ref('int_weather_daily_enriched') }}
)

select
    b.location_id,
    b.dates,
    round(
        (
            (case when b.temperature_2m_max is not null then 1 else 0 end) +
            (case when b.temperature_2m_min is not null then 1 else 0 end) +
            (case when b.temperature_2m_mean is not null then 1 else 0 end) +
            (case when b.precipitation_sum is not null then 1 else 0 end) +
            (case when b.rain_sum is not null then 1 else 0 end) +
            (case when b.snowfall_sum is not null then 1 else 0 end) +
            (case when b.sunshine_duration is not null then 1 else 0 end) +
            (case when b.wind_speed_10m_max is not null then 1 else 0 end)
        )::numeric / 8,
        2
    ) as completeness_score,
    r.abs_diff_temp_max,
    r.abs_diff_temp_min,
    r.abs_diff_precip,
    b.precip_hours,
    case
        when r.abs_diff_precip is null then 'missing_hourly'
        when coalesce(r.abs_diff_temp_max, 0) <= 2
            and coalesce(r.abs_diff_temp_min, 0) <= 2
            and coalesce(r.abs_diff_precip, 0) <= 5 then 'ok'
        else 'warning'
    end as reconciliation_status
from base b
    left join recon r
        on b.location_id = r.location_id
        and b.dates = r.dates
