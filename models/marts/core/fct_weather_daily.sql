with api as (
    select *
    from {{ ref('fct_weather_daily_api') }}
),
hourly as (
    select *
    from {{ ref('fct_weather_daily_hourly') }}
)

select
    coalesce(h.location_id, a.location_id) as location_id,
    coalesce(h.dates, a.dates) as dates,

    -- canonical metrics
    coalesce(h.temperature_2m_max, a.temperature_2m_max) as temperature_2m_max,
    coalesce(h.temperature_2m_min, a.temperature_2m_min) as temperature_2m_min,
    coalesce(h.temperature_2m_mean, a.temperature_2m_mean) as temperature_2m_mean,

    coalesce(h.rain_sum, a.rain_sum) as rain_sum,
    coalesce(h.precipitation_sum, a.precipitation_sum) as precipitation_sum,
    coalesce(h.snowfall_sum, a.snowfall_sum) as snowfall_sum,

    a.sunshine_duration as sunshine_duration,

    h.wind_speed_10m_max,
    h.wind_speed_10m_mean,
    h.precip_hours,
    h.rain_hours,
    h.snowfall_hours,

    case when h.temperature_2m_max is not null then 'hourly_derived'
         when a.temperature_2m_max is not null then 'api_daily'
         else null end as temperature_source,

    case when h.precipitation_sum is not null then 'hourly_derived'
         when a.precipitation_sum is not null then 'api_daily'
         else null end as precipitation_source,

    case when a.sunshine_duration is not null then 'api_daily'
         else null end as sunshine_source
from hourly h
full outer join api a
    on h.location_id = a.location_id
        and h.dates = a.dates