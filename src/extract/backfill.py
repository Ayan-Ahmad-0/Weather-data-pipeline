"""
Day 4: pulls historical hourly weather from Open-Meteo's archive endpoint
and reshapes each hour to match the live extractor's raw_response format,
so it can be loaded through the exact same load_records() function.
"""

from datetime import date, datetime, timezone

import requests

from src.extract.cities import get_cities
from src.load.load_to_postgres import load_records
from src.utils.logger import get_logger

logger = get_logger(__name__)

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
REQUEST_TIMEOUT_SECONDS = 15

HOURLY_VARS = "temperature_2m,wind_speed_10m,wind_direction_10m,weather_code,is_day"


def fetch_historical(city: dict, start_date: str, end_date: str) -> list[dict]:
    """Fetch hourly history for one city over [start_date, end_date]
    (YYYY-MM-DD, inclusive) and return records shaped like the live
    extractor's output."""
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "start_date": start_date,
        "end_date": end_date,
        "hourly": HOURLY_VARS,
        "timezone": "UTC",  # explicit UTC, matching fetch_weather.py — keeps
                             # observed_at comparable across live and backfilled rows
    }

    try:
        response = requests.get(ARCHIVE_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
        hourly = payload["hourly"]
    except (requests.RequestException, KeyError, ValueError) as exc:
        logger.error("Backfill failed for %s (%s to %s): %s", city["name"], start_date, end_date, exc)
        return []

    # Fixed once per call, not once per row — this is when the backfill
    # script actually ran, not the historical observed time. Using the
    # observed time here (as before) let old backfilled rows masquerade
    # as "the pipeline just ran" in dashboard freshness checks.
    pulled_at = datetime.now(timezone.utc).isoformat()

    records = []
    for i, timestamp in enumerate(hourly["time"]):
        reshaped_payload = {
            "current_weather": {
                "time": timestamp,
                "temperature": hourly["temperature_2m"][i],
                "windspeed": hourly["wind_speed_10m"][i],
                "winddirection": hourly["wind_direction_10m"][i],
                "weathercode": hourly["weather_code"][i],
                "is_day": hourly["is_day"][i],
            }
        }
        records.append({
            "city_name": city["name"],
            "country": city["country"],
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "observed_at": timestamp,
            "fetched_at": pulled_at,
            "source": "open-meteo-archive",
            "raw_response": reshaped_payload,
        })

    return records


def backfill_all_cities(start_date: str, end_date: str) -> int:
    total_loaded = 0
    for city in get_cities():
        records = fetch_historical(city, start_date, end_date)
        total_loaded += load_records(records)
        logger.info("Backfilled %s: %d hourly records", city["name"], len(records))
    return total_loaded


if __name__ == "__main__":
    # Example: last 5 days, ending today (archive data lags ~2 days behind live,
    # so the most recent 1-2 days may simply come back empty)
    today = date.today()
    start = today.replace(day=max(1, today.day - 5)).isoformat()
    end = today.isoformat()
    total = backfill_all_cities(start, end)
    print(f"Backfill complete: {total} records loaded")