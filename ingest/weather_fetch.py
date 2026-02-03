import os
import json
from pathlib import Path
import requests
import time
import pandas as pd
from pandas.errors import ParserError
from datetime import date, timedelta

REPO_ROOT = Path(__file__).resolve().parents[1]
LOCATIONS_CSV = REPO_ROOT / "seeds" / "seed_locations.csv"
RAW_DIR = REPO_ROOT / "data" / "raw"
OPEN_METEO_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain", 
    "snowfall", 
    "snow_depth",
    "wind_speed_10m",
]
DAILY_VARS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean", 
    "rain_sum", 
    "precipitation_sum", 
    "snowfall_sum", 
    "sunshine_duration",
]


def to_bool(x):
    return str(x).strip().lower() in {"true", "1", "yes", "y", "t"}

def load_active_locations():
    try:
        df = pd.read_csv(LOCATIONS_CSV)
    except FileNotFoundError as e:
        raise FileNotFoundError(
            f"Cannot find locations CSV at: {csv_path}. "
            f"Make sure it exists and you are running inside the repo."
        ) from e
    except ParserError as e:
        raise ParserError(
            f"CSV parse error in {csv_path}. "
            f"Common cause: a comma inside a field like name=Seattle, WA. "
            f'Fix by quoting: "Seattle, WA" or removing the comma.'
        ) from e
    
    required = {"location_id", "name", "state", "country", "latitude", "longitude", "timezone", "is_active"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"seed_locations.csv missing columns: {sorted(missing)}")
    
    df["is_active"] = df["is_active"].apply(to_bool)
    df = df[df["is_active"]].copy()

    if df.empty:
        raise ValueError("No active locations found. Set is_active=true for at least one row.")

    if df["latitude"].isna().any() or df["longitude"].isna().any():
        raise ValueError("Some active rows have null latitude/longitude.")

    return df

def default_date_window():
    lookback_days = int(os.getenv("WEATHER_LOOKBACK_DAYS", "90"))
    end_date = date.today() - timedelta(days=5) # 5-day delay 
    start_date = end_date - timedelta(days=lookback_days)
    return start_date, end_date

def build_params(row, start_date: date, end_date: date):
    return {
        "latitude": float(row["latitude"]),
        "longitude": float(row["longitude"]),
        "timezone": str(row["timezone"]),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "hourly": ",".join(HOURLY_VARS),
        "daily": ",".join(DAILY_VARS),
    }

def get_json_with_retry(url, params, timeout_s=60, max_retries=5):
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout_s)
            # Codes: 429 (rate limiting), 5xx (server/gateway problems)
            if r.status_code in (429, 500, 502, 503, 504):
                wait_s = min(60, 2 ** attempt)
                print(f"HTTP {r.status_code} attempt {attempt}/{max_retries}; retry in {wait_s}s. URL={r.url}")
                time.sleep(wait_s)
                continue

            r.raise_for_status()

            data = r.json()

            if not isinstance(data, dict) or "latitude" not in data or "longitude" not in data:
                raise RuntimeError(f"Unexpected response shape. URL={r.url}")

            return data
        except requests.exceptions.Timeout:
            wait_s = min(60, 2 ** attempt)
            print(f"Timeout attempt {attempt}/{max_retries}; retry in {wait_s}s.")
            time.sleep(wait_s)

        except requests.exceptions.RequestException as e:
            # Includes HTTPError, ConnectionError, etc.
            raise RuntimeError(f"Request failed: {e}") from e

        except ValueError as e:
            # JSON decode error
            raise RuntimeError(f"Invalid JSON returned. URL={getattr(r, 'url', url)}") from e

    raise RuntimeError(f"Exceeded retries for url={url}")

def fetch_by_location(row, start_date, end_date):
    params = build_params(row, start_date, end_date)
    responses = get_json_with_retry(OPEN_METEO_URL, params=params)

    return {
        "location_id": row["location_id"],
        "name": row["name"],
        "country": row.get("country", None),
        "timezone": row["timezone"],
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "ingested_at": date.today().isoformat(),
        "payload": responses,
        "request_params": params, 
    }

def write_jsonl(records, out_path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def main():
    locations = load_active_locations()
    start_date, end_date = default_date_window()

    results = []

    for _, row in locations.iterrows():
        print(f"Fetching {row['location_id']} ({row['name']}) {start_date} → {end_date}")
        row_data = fetch_by_location(row, start_date, end_date)
        results.append(row_data)
    
    out_file = RAW_DIR / f"weather_archive_{start_date}_{end_date}.jsonl"
    write_jsonl(results, out_file)

    print(f"Complete writing raw JSONL: {out_file}")

if __name__ == '__main__':
    main()