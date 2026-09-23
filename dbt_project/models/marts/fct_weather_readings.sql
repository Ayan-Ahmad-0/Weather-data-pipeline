-- Grain: one row per city per observed timestamp.
select
    raw_id,
    city_name,
    observed_at,
    fetched_at,
    temperature_c,
    windspeed_kmh,
    wind_direction_deg,
    weather_code,
    is_day,
    data_source
from {{ ref('stg_weather_readings') }}