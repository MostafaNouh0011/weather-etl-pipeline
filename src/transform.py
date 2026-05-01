import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

def parse_response(raw: dict) -> dict | None:
    try:
        loc = raw.get("location", {})
        cur = raw.get("current", {})
        localtime_str = loc.get("localtime", "")
        try:
            # Store as ISO-8601 string so the value survives JSON XCom serialization.
            # Postgres TIMESTAMP columns accept ISO strings via psycopg2.
            local_time = datetime.strptime(localtime_str, "%Y-%m-%d %H:%M").isoformat()
        except (ValueError, TypeError):
            local_time = None

        return {
            "city":                loc.get("name"),
            "country":             loc.get("country"),
            "region":              loc.get("region"),
            "lat":                 float(loc.get("lat", 0)),
            "lon":                 float(loc.get("lon", 0)),
            "local_time":          local_time,
            "temperature_c":       cur.get("temperature"),
            "feels_like_c":        cur.get("feelslike"),
            "humidity":            cur.get("humidity"),
            "wind_speed_kmh":      cur.get("wind_speed"),
            "wind_direction":      cur.get("wind_dir"),
            "weather_description": (cur.get("weather_descriptions") or ["N/A"])[0],
            "uv_index":            cur.get("uv_index"),
            "visibility":          cur.get("visibility"),
            "pressure":            cur.get("pressure"),
            "cloudcover":          cur.get("cloudcover"),
            "is_day":              cur.get("is_day") == "yes",
        }
    except Exception as e:
        logger.error(f"Parse error: {e}")
        return None

def transform(raw_responses: list[dict]) -> pd.DataFrame:
    records = [parse_response(r) for r in raw_responses]
    records = [r for r in records if r is not None]
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    df.dropna(subset=["city", "temperature_c"], inplace=True)
    df["city"]    = df["city"].str.strip().str.title()
    df["country"] = df["country"].str.strip().str.title()
    logger.info(f"Transform complete: {len(df)} clean records")
    return df