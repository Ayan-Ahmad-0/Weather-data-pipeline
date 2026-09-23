"""
Day 3: takes extractor output and persists it to raw.weather_readings.

Kept deliberately dumb — this module doesn't know or care how the records
were fetched. It just upserts whatever list of dicts it's given, as long
as they match the schema's columns. That's what lets backfill.py (Day 4)
reuse this exact function instead of writing its own load path.
"""

from psycopg2.extras import execute_values

from src.extract.fetch_weather import fetch_all_cities
from src.utils.db import get_connection
from src.utils.logger import get_logger

logger = get_logger(__name__)

import json

INSERT_SQL = """
    INSERT INTO raw.weather_readings
        (city_name, country, latitude, longitude, observed_at, fetched_at, source, raw_response)
    VALUES %s
    ON CONFLICT (city_name, observed_at) DO NOTHING
"""


def load_records(records: list[dict]) -> int:
    """Upsert records into raw.weather_readings. Returns the count of rows
    actually inserted (duplicates on city_name+observed_at are silently
    skipped — that's what makes re-running the pipeline safe)."""
    if not records:
        logger.warning("No records to load — skipping.")
        return 0

    values = [
        (
            r["city_name"],
            r["country"],
            r["latitude"],
            r["longitude"],
            r["observed_at"],
            r["fetched_at"],
            r["source"],
            json.dumps(r["raw_response"]),
        )
        for r in records
    ]

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(cur, INSERT_SQL, values)
                inserted = cur.rowcount
        logger.info("Loaded %d/%d records (rest were duplicates, skipped).", inserted, len(records))
        return inserted
    finally:
        conn.close()


if __name__ == "__main__":
    records = fetch_all_cities()
    load_records(records)