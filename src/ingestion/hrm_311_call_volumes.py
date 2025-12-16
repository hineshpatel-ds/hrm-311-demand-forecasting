import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import text

from src.utils.db import get_engine, ensure_schemas

DATASET_ID = "8bfd88fc3de041c894cb69e5c62304fb"  # HRM 311 Call Volumes (ArcGIS Hub)

def download_csv(out_dir: str = "data/raw") -> Path:
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    # ArcGIS Hub download endpoint pattern (CSV export)
    url = (
        f"https://opendata.arcgis.com/api/v3/datasets/{DATASET_ID}_0/"
        "downloads/data?format=csv&spatialRefId=4326"
    )

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = Path(out_dir) / f"hrm_311_call_volumes_{ts}.csv"

    r = requests.get(url, timeout=120)
    r.raise_for_status()
    out_path.write_bytes(r.content)

    return out_path

def load_to_postgres(csv_path: Path, table_name: str = "raw.raw_311_call_volumes") -> int:
    ensure_schemas()
    engine = get_engine()

    df = pd.read_csv(csv_path)

    # Add ingestion metadata (industry standard: lineage + auditability)
    df["_ingested_at_utc"] = datetime.now(timezone.utc)

    # Raw layer strategy: replace each run OR append with partitioning.
    # For Day2, we do a safe "replace" to keep it simple and deterministic.
    df.to_sql(table_name.split(".")[1], engine, schema="raw", if_exists="replace", index=False)

    # Record ingestion log
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS raw.ingestion_log (
                id SERIAL PRIMARY KEY,
                dataset TEXT NOT NULL,
                table_name TEXT NOT NULL,
                ingested_at_utc TIMESTAMPTZ NOT NULL,
                rows_loaded INTEGER NOT NULL
            );
        """))
        conn.execute(
            text("""
                INSERT INTO raw.ingestion_log(dataset, table_name, ingested_at_utc, rows_loaded)
                VALUES (:dataset, :table_name, :ingested_at_utc, :rows_loaded);
            """),
            {
                "dataset": "hrm_311_call_volumes",
                "table_name": table_name,
                "ingested_at_utc": datetime.now(timezone.utc),
                "rows_loaded": len(df),
            },
        )

    return len(df)

if __name__ == "__main__":
    csv_file = download_csv()
    rows = load_to_postgres(csv_file)
    print(f"✅ Loaded {rows} rows into raw.raw_311_call_volumes")
