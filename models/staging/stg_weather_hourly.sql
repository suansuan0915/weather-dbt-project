with src as (
    select *
    from {{ source("bronze", "weather_hourly") }}
)

select
    cast(location_id as text) as location_id,
    cast(observed_at, timestamp) as date_hour,
    cast(ingested_at as timestamp) as ingested_at,
    cast(p_date as date) as p_date,
    cast(temperature_2m as double precision) as temperature_2m,
    cast(relative_humidity_2m as bigint) as relative_humidity_2m,
    cast(precipitation as double precision) as precipitation,
    cast(rain as double precision) as rain,
    cast(snowfall as double precision) as snowfall,
    cast(snow_depth as double precision) as snow_depth,
    cast(wind_speed_10m as double precision) as wind_speed_10m
from src
;