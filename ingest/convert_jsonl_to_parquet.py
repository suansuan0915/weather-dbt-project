import json
import shutil
from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = REPO_ROOT / "data" / "landing"
BRONZE_DIR = REPO_ROOT / "data" / "bronze"
OUT_HOURLY = BRONZE_DIR / "weather_hourly"
OUT_DAILY = BRONZE_DIR / "weather_daily"

def _records_from_jsonl(path):
    records = []

    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Invalid JSON on {path} line {line_no}") from e
            
    return records

def _explode_time_series(record, time_series, time_col_name):
    payload = record.get("payload") or {}
    ts = payload.get(time_series) or {}
    times = ts.get("time")

    if not times:
        return pd.DataFrame()
    
    df = pd.DataFrame({time_col_name: times})

    for k, v in ts.items():
        if k != "time":
            df[k] = v
    
    df["location_id"] = record.get("location_id")
    df["ingested_at"] = record.get("ingested_at")

    df[time_col_name] = pd.to_datetime(df[time_col_name], errors="coerce")
    df["ingested_at"] = pd.to_datetime(df["ingested_at"], errors="coerce", utc=True)

    return df

def jsonl_to_parquet(path):
    hourly_records = []
    daily_records = []

    jsonl_files = sorted(path.glob("*.jsonl"))
    if not jsonl_files:
        raise FileNotFoundError(f"No .jsonl files found under {path}")
    
    for fp in jsonl_files:
        print(f"Reading: {fp}")
        jsonl_records = _records_from_jsonl(fp)

        for record in jsonl_records:
            h = _explode_time_series(record, "hourly", "observed_at")
            if not h.empty:
                hourly_records.append(h)

            d = _explode_time_series(record, "daily", "date")
            if not d.empty:
                daily_records.append(d)

    hourly_df = pd.concat(hourly_records, ignore_index=True) if hourly_records else pd.DataFrame()
    daily_df = pd.concat(daily_records, ignore_index=True) if daily_records else pd.DataFrame()
    return hourly_df, daily_df
    
def write_partitioned_parquet(df, out_path, partition_col):
    if out_path.exists():
        shutil.rmtree(out_path)
    out_path.mkdir(parents=True, exist_ok=True)

    if df.empty:
        print(f"Nothing to write for {out_path} (empty DataFrame).")
        return
    
    if partition_col == "date":
        df["p_date"] = pd.to_datetime(df["date"]).dt.date.astype(str)
    elif partition_col == "observed_at":
        df["p_date"] = pd.to_datetime(df["observed_at"]).dt.date.astype(str)
    else:
        raise ValueError("partition_col must be 'date' or 'observed_at'")
    
    df.to_parquet(
        path=out_path, 
        index= False,
        engine="pyarrow",
        partition_cols=["p_date"])
    print(f"Complete writing Parquet dataset to: {out_path}")

def main():
    BRONZE_DIR.mkdir(parents=True, exist_ok=True) 

    hourly_df, daily_df = jsonl_to_parquet(RAW_DIR)

    write_partitioned_parquet(hourly_df, OUT_HOURLY, partition_col="observed_at")
    write_partitioned_parquet(daily_df, OUT_DAILY, partition_col="date")

if __name__ == "__main__":
    main()
