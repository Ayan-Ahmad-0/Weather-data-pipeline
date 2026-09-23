CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.weather_readings (
    id            BIGSERIAL PRIMARY KEY,
    city_name     TEXT NOT NULL,
    country       TEXT,
    latitude      NUMERIC(8, 4) NOT NULL,
    longitude     NUMERIC(8, 4) NOT NULL,
    observed_at   TIMESTAMPTZ NOT NULL,
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    source        TEXT NOT NULL DEFAULT 'open-meteo',
    raw_response  JSONB NOT NULL,
    UNIQUE (city_name, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_weather_readings_city_time
    ON raw.weather_readings (city_name, observed_at DESC);