"""
Day 2: pulls current weather for all tracked cities from Open-Meteo.

No Postgres writes here on purpose — this module's only job is to return
clean, uniform records. Day 3's loader takes its output and persists it.
Keeping extract and load separate means either can be tested/re-run alone.
"""

import time
from datetime import datetime, timezone

import requests

from src.extract.cities import get_cities
from src.utils.logger import get_logger

logger = get_logger(__name__)

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
REQUEST_TIMEOUT_SECONDS = 10
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2


def fetch_current_weather(city: dict) -> dict | None:
    """Fetch current weather for one city. Returns a record dict, or None
    if every retry failed — callers must handle the None case, since one
    city's outage shouldn't stop the other nine."""
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "current_weather": True,
        "timezone": "UTC",  # explicit UTC — "auto" returns each city's local
                             # time with no offset, which Postgres then
                             # misreads as UTC, corrupting downstream comparisons
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(params=params, url=OPEN_METEO_URL, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            payload = response.json()
            current = payload["current_weather"]

            return {
                "city_name": city["name"],
                "country": city["country"],
                "latitude": city["latitude"],
                "longitude": city["longitude"],
                "observed_at": current["time"],       # now genuinely UTC
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "source": "open-meteo",
                "raw_response": payload,
            }

        except (requests.RequestException, KeyError, ValueError) as exc:
            logger.warning(
                "Fetch failed for %s (attempt %d/%d): %s",
                city["name"], attempt, MAX_RETRIES, exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    logger.error("Giving up on %s after %d attempts", city["name"], MAX_RETRIES)
    return None


def fetch_all_cities() -> list[dict]:
    """Fetch every tracked city, skipping any that failed all retries."""
    records = []
    for city in get_cities():
        record = fetch_current_weather(city)
        if record is not None:
            records.append(record)

    logger.info("Fetched %d/%d cities successfully", len(records), len(get_cities()))
    return records


if __name__ == "__main__":
    results = fetch_all_cities()
    for r in results:
        print(f"{r['city_name']:<12} {r['raw_response']['current_weather']['temperature']}°C "
              f"@ {r['observed_at']}")