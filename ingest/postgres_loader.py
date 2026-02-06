import os
import argparse
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

def get_engine():
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db   = os.environ.get("POSTGRES_DB", "postgres")
    user = os.environ.get("POSTGRES_USER", "postgres")
    pwd  = os.environ.get("POSTGRES_PASSWORD")

    if not pwd:
        raise RuntimeError(
            "Missing POSTGRES_PASSWORD (or POSTGRES_URL). "
            "Set it in your env or .env (gitignored)."
        )
    
    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    
    return create_engine(url, pool_pre_ping=True)

def ensure_schema(engine, schema: str):
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

def load_parquet_to_postgres(
    engine,
    parquet_path: str,
    schema: str,
    table: str,
    mode="append",
    chunksize=50_000,
):
    p = Path(parquet_path)
    if not p.exists():
        raise FileNotFoundError(f"Parquet path not found: {parquet_path}")
    
    df = pd.read_parquet(parquet_path)

    if df.empty:
        print(f"[SKIP] {schema}.{table}: dataframe is empty.")
        return
    
    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists=mode,   # "append" or "replace"
        index=False,
        chunksize=chunksize,
        method="multi",
    )
    print(f"[OK] Loaded {len(df):,} rows into {schema}.{table} (mode={mode}).")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default="bronze")
    parser.add_argument("--mode", choices=["append", "replace"], default="append")
    parser.add_argument("--hourly-path", default="data/bronze/weather_hourly")
    parser.add_argument("--daily-path", default="data/bronze/weather_daily")
    args = parser.parse_args()

    engine = get_engine()
    ensure_schema(engine, args.schema)

    load_parquet_to_postgres(
        engine, args.hourly_path, args.schema, "weather_hourly", mode=args.mode
    )
    load_parquet_to_postgres(
        engine, args.daily_path, args.schema, "weather_daily", mode=args.mode
    )

if __name__ == "__main__":
    main()