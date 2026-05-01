-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Raw layer: every API response lands here
CREATE TABLE IF NOT EXISTS raw.weather_raw (
    id                   SERIAL PRIMARY KEY,
    city                 VARCHAR(100)  NOT NULL,
    country              VARCHAR(100),
    region               VARCHAR(100),
    lat                  FLOAT,
    lon                  FLOAT,
    local_time           TIMESTAMP,
    temperature_c        FLOAT,
    feels_like_c         FLOAT,
    humidity             INT,
    wind_speed_kmh       FLOAT,
    wind_direction       VARCHAR(10),
    weather_description  VARCHAR(200),
    uv_index             INT,
    visibility           INT,
    pressure             INT,
    cloudcover           INT,
    is_day               BOOLEAN,
    ingested_at          TIMESTAMP DEFAULT NOW()
);

-- Analytics layer: transformed and aggregated data
CREATE TABLE IF NOT EXISTS analytics.weather_summary (
    city                 VARCHAR(100),
    country              VARCHAR(100),
    record_date          DATE,
    avg_temperature_c    FLOAT,
    max_temperature_c    FLOAT,
    min_temperature_c    FLOAT,
    avg_humidity         FLOAT,
    avg_wind_speed_kmh   FLOAT,
    dominant_condition   VARCHAR(200),
    total_records        INT,
    last_updated         TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (city, record_date)
);