with daily_src as (
    select 
        location_id,
        dates,
        ingested_at,
        p_date,
        temperature_2m_max,
        temperature_2m_min,
        temperature_2m_mean,
        rain_sum,
        precipitation_sum,
        snowfall_sum,
        sunshine_duration
    from {{ ref('stg_bronze__weather_daily') }}
),

hourly_rollup as (
    select
        location_id,
        observed_at::date as dates,    

        max(temperature_2m) as temperature_2m_max_from_hourly,
        min(temperature_2m) as temperature_2m_min_from_hourly,
        avg(temperature_2m) as temperature_2m_mean_from_hourly,

        sum(coalesce(rain, 0)) as rain_sum_from_hourly,
        sum(coalesce(precipitation, 0)) as precipitation_sum_from_hourly,
        sum(coalesce(snowfall, 0)) as snowfall_sum_from_hourly,

        max(wind_speed_10m) as wind_speed_10m_max,
        avg(wind_speed_10m) as wind_speed_10m_mean,

        sum(case when precipitation > 0 then 1 else 0 end) as precip_hours,
        sum(case when rain > 0 then 1 else 0 end) as rain_hours,
        sum(case when snowfall > 0 then 1 else 0 end) as snowfall_hours,

        max(ingested_at) as hourly_latest_ingested_at
    from {{ ref('int_weather_hourly_enriched') }}
    group by 1, 2
)

select
    d.location_id,
    d.dates,
    d.ingested_at as daily_ingested_at,
    d.p_date as daily_p_date,

    --  API daily metrics
    d.temperature_2m_max,
    d.temperature_2m_min,
    d.temperature_2m_mean,
    d.rain_sum,
    d.precipitation_sum,
    d.snowfall_sum,
    d.sunshine_duration,

    -- hourly-derived daily metrics
    h.temperature_2m_max_from_hourly,
    h.temperature_2m_min_from_hourly,
    h.temperature_2m_mean_from_hourly,
    h.rain_sum_from_hourly,
    h.precipitation_sum_from_hourly,
    h.snowfall_sum_from_hourly,
    h.wind_speed_10m_max,
    h.wind_speed_10m_mean,
    h.precip_hours,
    h.rain_hours,
    h.snowfall_hours,
    h.hourly_latest_ingested_at,

    -- comparison
    (d.temperature_2m_max - h.temperature_2m_max_from_hourly) as diff_temp_max_api_vs_hourly,
    (d.temperature_2m_min - h.temperature_2m_min_from_hourly) as diff_temp_min_api_vs_hourly,
    (d.precipitation_sum - h.precipitation_sum_from_hourly) as diff_precip_api_vs_hourly
from daily_src d
left join hourly_rollup h
    on d.location_id = h.location_id
        and d.dates = h.dates