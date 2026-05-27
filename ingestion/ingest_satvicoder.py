"""
Ingest satvicoder/call-center-data Kaggle CSV into DuckDB raw.raw_satvicoder_calls.
Expects file in /opt/airflow/data/raw/satvicoder/ after Kaggle API download.
"""

import os
import glob
import duckdb
import pandas as pd

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/data/call_center.duckdb")
RAW_DIR = "/opt/airflow/data/raw/satvicoder"


def run():
    csv_files = glob.glob(os.path.join(RAW_DIR, "*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV found in {RAW_DIR} — run download_satvicoder first")

    csv_path = csv_files[0]
    print(f"Reading: {csv_path}")

    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} rows | columns: {df.columns.tolist()}")

    with duckdb.connect(DUCKDB_PATH) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute("DROP TABLE IF EXISTS raw.raw_satvicoder_calls")
        con.execute("CREATE TABLE raw.raw_satvicoder_calls AS SELECT * FROM df")
        row_count = con.execute("SELECT COUNT(*) FROM raw.raw_satvicoder_calls").fetchone()[0]

    print(f"raw.raw_satvicoder_calls loaded: {row_count} rows")
