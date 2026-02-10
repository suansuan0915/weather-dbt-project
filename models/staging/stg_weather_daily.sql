with src as (
    select *
    from {{ source("bronze", "weather_daily") }}
)

select
    cast(location_id as text) as location_id,
    cast(`date`, timestamp) as dates,
    cast(ingested_at as timestamp) as ingested_at,
    cast(p_date as date) as p_date,
    cast(temperature_2m_max as double precision) as temperature_2m_max,
    cast(temperature_2m_min as double precision) as temperature_2m_min, 
    cast(temperature_2m_mean as double precision) as temperature_2m_mean,
    cast(rain_sum as double precision) as rain_sum,
    cast(precipitation_sum as double precision) as precipitation_sum,
    cast(snowfall_sum as double precision) as snowfall_sum,
    cast(sunshine_duration as double precision) as sunshine_duration
from src
;