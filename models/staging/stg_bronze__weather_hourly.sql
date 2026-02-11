with src as (
    select *
    from {{ source("bronze", "weather_hourly") }}
),
typed as (
    select
        cast(location_id as text) as location_id,
        cast(observed_at as timestamp) as observed_at,
        cast(ingested_at as timestamp) as ingested_at,
        cast(p_date as date) as p_date,
        cast(temperature_2m as double precision) as temperature_2m,
        cast(relative_humidity_2m as double precision) as relative_humidity_2m,
        cast(precipitation as double precision) as precipitation,
        cast(rain as double precision) as rain,
        cast(snowfall as double precision) as snowfall,
        cast(snow_depth as double precision) as snow_depth,
        cast(wind_speed_10m as double precision) as wind_speed_10m
    from src
),
dedup as (
    select *
    from (
        select 
            *,
            row_number() over(
                partition by location_id, observed_at 
                order by ingested_at desc, p_date desc) as ranks
        from typed
    ) t
    where ranks = 1
)

select 
        location_id,
        observed_at,
        ingested_at,
        p_date,
        temperature_2m,
        relative_humidity_2m,
        precipitation,
        rain,
        snowfall,
        snow_depth,
        wind_speed_10m
from dedup