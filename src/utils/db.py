import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def get_engine():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set. Copy .env.example to .env and set it")

    return create_engine(db_url)

def ensure_schemas():
    engine =get_engine()
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS mart"))

def load_mart_series() -> pd.DataFrame:
    """Load the forecasting-ready 30-min mart, indexed by bucket_ts."""
    engine = get_engine()
    df = pd.read_sql(
        """
        SELECT bucket_ts, offered
        FROM mart.mart_311_call_volume_30m
        ORDER BY bucket_ts
        """,
        engine,
        parse_dates=["bucket_ts"],
    )
    df = df.dropna()
    df["bucket_ts"] = pd.to_datetime(df["bucket_ts"], utc=True)
    return df.set_index("bucket_ts")

