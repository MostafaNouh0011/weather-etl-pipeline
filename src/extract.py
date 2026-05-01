import os
import logging
import requests

logger = logging.getLogger(__name__)

API_KEY  = os.getenv("WEATHERSTACK_API_KEY")
BASE_URL = "http://api.weatherstack.com/current"

if not API_KEY:
    logger.warning("WEATHERSTACK_API_KEY is not set; every fetch will fail")

CITIES = [
    "Cairo", "Alexandria", "Dubai", "London",
    "New York", "Paris", "Tokyo", "Sydney",
    "Berlin", "Toronto"
]

def fetch_weather(city: str) -> dict | None:
    params = {"access_key": API_KEY, "query": city, "units": "m"}
    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        if not data.get("success", True):
            logger.warning(f"API error for {city}: {data.get('error')}")
            return None
        logger.info(f"Fetched: {city}")
        return data
    except Exception as e:
        logger.error(f"Failed to fetch {city}: {e}")
        return None

def extract_all_cities() -> list[dict]:
    logger.info(f"Extracting weather for {len(CITIES)} cities")
    results = [fetch_weather(city) for city in CITIES]
    results = [r for r in results if r is not None]
    logger.info(f"Extraction done: {len(results)}/{len(CITIES)} succeeded")
    return results