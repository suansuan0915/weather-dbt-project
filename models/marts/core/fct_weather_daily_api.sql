select 
    location_id,
    dates::date as dates,
    temperature_2m_max,
    temperature_2m_min,
    temperature_2m_mean,
    rain_sum,
    precipitation_sum,
    snowfall_sum,
    sunshine_duration
from {{ ref('stg_bronze__weather_daily') }}