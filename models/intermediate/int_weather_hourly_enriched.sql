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
    wind_speed_10m,
    case
        when temperature_2m is null then null
        when temperature_2m <= 0 then 'freezing'
        when temperature_2m <= 15 then 'cold'
        when temperature_2m <= 25 then 'mild'
        else 'hot'
    end as temp_bucket,
    case 
        when relative_humidity_2m is null then null
        when relative_humidity_2m <= 30 then 'dry'
        when relative_humidity_2m <= 50 then 'mild'
        when relative_humidity_2m <= 60 then 'moderately humid'
        when relative_humidity_2m <= 70 then 'humid'
        else 'very humid'
    end as humidity_bucket,
    case 
        when wind_speed_10m is null then null
        when wind_speed_10m <= 19 then 'light'
        when wind_speed_10m <= 38 then 'moderate'
        when wind_speed_10m <= 61 then 'strong'
        else 'gale'
    end as wind_bucket,
    case 
        when precipitation is null then null
        when precipitation < 0.1 then 'no precipitation'
        when precipitation <= 2.5 then 'very light'
        when precipitation <= 7.5 then 'light'
        when precipitation <= 30 then 'moderate'
        else 'heavy'
    end as precipitation_bucket,
    case 
        when precipitation is null then null
        else precipitation > 0 
    end as is_precipitating,
    case 
        when rain is null then null
        else rain > 0
    end as is_rain,
    case when snowfall is null then null
        else snowfall > 0 
    end as is_snowfall
from {{ ref("stg_bronze__weather_hourly") }}