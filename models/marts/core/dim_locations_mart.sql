{{ config(alias='dim_locations') }}

select
    location_id,
    name as location_name,
    state,
    country,
    latitude,
    longitude,
    timezone,
    is_active
from {{ ref('dim_locations') }}
