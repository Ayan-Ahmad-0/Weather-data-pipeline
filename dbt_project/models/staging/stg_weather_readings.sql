-- Flattens the raw JSONB payload into typed columns.
-- Grain: one row per (city, observed_at) — same grain as the raw table.

with source as (

    select * from raw.weather_readings

),

flattened as (

    select
        id                                                          as raw_id,
        city_name,
        country,
        latitude,
        longitude,
        observed_at,
        fetched_at,
        source                                                      as data_source,
        (raw_response -> 'current_weather' ->> 'temperature')::numeric   as temperature_c,
        (raw_response -> 'current_weather' ->> 'windspeed')::numeric     as windspeed_kmh,
        (raw_response -> 'current_weather' ->> 'winddirection')::numeric as wind_direction_deg,
        (raw_response -> 'current_weather' ->> 'weathercode')::int       as weather_code,
        (raw_response -> 'current_weather' ->> 'is_day')::boolean        as is_day

    from source

)

select * from flattened