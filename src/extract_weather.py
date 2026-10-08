import argparse
import calendar
import csv
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src import config
from src.storage import get_s3_client, object_exists, put_json

API_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARS = "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation"
LOCATIONS_FILE = Path(__file__).resolve().parent.parent / "config" / "locations.csv"
PAUSE_SECONDS = 0.5


def load_locations():
    with open(LOCATIONS_FILE, newline="") as f:
        return list(csv.DictReader(f))


def build_session():
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def s3_key(location_id, year):
    return (
        f"raw/source=open_meteo/location={location_id}/year={year}/"
        f"weather_{location_id}_{year}.json"
    )


def fetch_year(session, location, year):
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "hourly": HOURLY_VARS,
    }
    response = session.get(API_URL, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    expected_hours = (366 if calendar.isleap(year) else 365) * 24
    received_hours = len(data["hourly"]["time"])
    if received_hours != expected_hours:
        raise ValueError(
            f"{location['location_id']} {year}: expected {expected_hours} hours, got {received_hours}"
        )

    return {
        "_meta": {
            "source": "open-meteo-archive",
            "request_url": response.url,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "location_id": location["location_id"],
            "year": year,
            "hours": received_hours,
        },
        "response": data,
    }


def main():
    parser = argparse.ArgumentParser(description="Extract hourly weather from Open-Meteo into the S3 raw zone")
    parser.add_argument("--start-year", type=int, default=2002)
    parser.add_argument("--end-year", type=int, default=2018)
    parser.add_argument("--force", action="store_true", help="download again even if the object already exists")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    s3 = get_s3_client(config.AWS_PROFILE, config.AWS_REGION)
    session = build_session()

    uploaded = skipped = failed = 0
    for location in load_locations():
        for year in range(args.start_year, args.end_year + 1):
            key = s3_key(location["location_id"], year)
            if not args.force and object_exists(s3, config.S3_BUCKET, key):
                skipped += 1
                logging.info("skip (exists): %s", key)
                continue
            try:
                payload = fetch_year(session, location, year)
                put_json(s3, config.S3_BUCKET, key, payload)
                uploaded += 1
                logging.info("uploaded: %s", key)
            except Exception:
                failed += 1
                logging.exception("failed: %s %s", location["location_id"], year)
            time.sleep(PAUSE_SECONDS)

    logging.info("summary: uploaded=%d skipped=%d failed=%d", uploaded, skipped, failed)
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
