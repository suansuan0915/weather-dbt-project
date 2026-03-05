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

def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def ensure_unique_index(engine, schema: str, table: str, key_cols: list[str]):
    idx_name = f"uq_{table}_{'_'.join(key_cols)}"
    key_list = ", ".join(quote_ident(c) for c in key_cols)
    with engine.begin() as conn:
        conn.execute(
            text(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {quote_ident(idx_name)} "
                f"ON {quote_ident(schema)}.{quote_ident(table)} ({key_list});"
            )
        )

def load_parquet_to_postgres(
    engine,
    parquet_path: str,
    schema: str,
    table: str,
    key_cols: list[str],
    mode="upsert",
    chunksize=50_000,
):
    p = Path(parquet_path)
    if not p.exists():
        raise FileNotFoundError(f"Parquet path not found: {parquet_path}")
    
    df = pd.read_parquet(parquet_path)

    if df.empty:
        print(f"[SKIP] {schema}.{table}: dataframe is empty.")
        return

    # Create target table if needed (schema from DataFrame, 0 rows).
    df.head(0).to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
    )
    ensure_unique_index(engine, schema, table, key_cols)

    if mode == "replace":
        df.to_sql(
            name=table,
            con=engine,
            schema=schema,
            if_exists="replace",
            index=False,
            chunksize=chunksize,
            method="multi",
        )
        ensure_unique_index(engine, schema, table, key_cols)
        print(f"[OK] Loaded {len(df):,} rows into {schema}.{table} (mode=replace).")
        return

    if mode == "append":
        df.to_sql(
            name=table,
            con=engine,
            schema=schema,
            if_exists="append",
            index=False,
            chunksize=chunksize,
            method="multi",
        )
        print(f"[OK] Loaded {len(df):,} rows into {schema}.{table} (mode=append).")
        return

    # Default: idempotent upsert with conflict keys.
    stage_table = f"__stg_{table}"
    df.to_sql(
        name=stage_table,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
        chunksize=chunksize,
        method="multi",
    )

    quoted_cols = [quote_ident(c) for c in df.columns]
    col_list = ", ".join(quoted_cols)
    key_list = ", ".join(quote_ident(c) for c in key_cols)
    update_cols = [c for c in df.columns if c not in key_cols]
    set_clause = ", ".join(f"{quote_ident(c)} = EXCLUDED.{quote_ident(c)}" for c in update_cols)
    conflict_action = f"DO UPDATE SET {set_clause}" if set_clause else "DO NOTHING"

    upsert_sql = f"""
        INSERT INTO {quote_ident(schema)}.{quote_ident(table)} ({col_list})
        SELECT {col_list}
        FROM {quote_ident(schema)}.{quote_ident(stage_table)}
        ON CONFLICT ({key_list}) {conflict_action};
    """
    drop_stage_sql = f'DROP TABLE IF EXISTS {quote_ident(schema)}.{quote_ident(stage_table)};'

    with engine.begin() as conn:
        conn.execute(text(upsert_sql))
        conn.execute(text(drop_stage_sql))

    print(f"[OK] Loaded {len(df):,} rows into {schema}.{table} (mode=upsert).")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default="bronze")
    parser.add_argument("--mode", choices=["upsert", "append", "replace"], default="upsert")
    parser.add_argument("--hourly-path", default="data/bronze/weather_hourly")
    parser.add_argument("--daily-path", default="data/bronze/weather_daily")
    args = parser.parse_args()

    engine = get_engine()
    ensure_schema(engine, args.schema)

    load_parquet_to_postgres(
        engine,
        args.hourly_path,
        args.schema,
        "weather_hourly",
        key_cols=["location_id", "observed_at"],
        mode=args.mode,
    )
    load_parquet_to_postgres(
        engine,
        args.daily_path,
        args.schema,
        "weather_daily",
        key_cols=["location_id", "date"],
        mode=args.mode,
    )

if __name__ == "__main__":
    main()
