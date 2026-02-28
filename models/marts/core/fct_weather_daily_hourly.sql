with hourly_daily as (
    select
        location_id,
        observed_at::date as dates,

        max(temperature_2m) as temperature_2m_max,
        min(temperature_2m) as temperature_2m_min,
        avg(temperature_2m) as temperature_2m_mean,

        sum(coalesce(rain, 0)) as rain_sum,
        sum(coalesce(precipitation, 0)) as precipitation_sum,
        sum(coalesce(snowfall, 0)) as snowfall_sum,

        max(wind_speed_10m) as wind_speed_10m_max,
        avg(wind_speed_10m) as wind_speed_10m_mean,

        sum(case when precipitation > 0 then 1 else 0 end) as precip_hours,
        sum(case when rain > 0 then 1 else 0 end) as rain_hours,
        sum(case when snowfall > 0 then 1 else 0 end) as snowfall_hours

    from {{ ref('int_weather_hourly_enriched') }}
    group by 1, 2
)

select 
    location_id,
    dates,
    temperature_2m_max,
    temperature_2m_min,
    temperature_2m_mean,
    rain_sum,
    precipitation_sum,
    snowfall_sum,
    wind_speed_10m_max,
    wind_speed_10m_mean,
    precip_hours,
    rain_hours,
    snowfall_hours
from hourly_daily