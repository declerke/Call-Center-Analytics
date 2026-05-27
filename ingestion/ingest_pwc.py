"""
Ingest gayatriwagadre/pwc-call-centre-analysis Kaggle XLSX into DuckDB raw.raw_pwc_calls.
Expects file in /opt/airflow/data/raw/pwc/ after Kaggle API download.
"""

import os
import glob
import duckdb
import pandas as pd

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/data/call_center.duckdb")
RAW_DIR = "/opt/airflow/data/raw/pwc"


def run():
    xlsx_files = glob.glob(os.path.join(RAW_DIR, "*.xlsx"))
    if not xlsx_files:
        raise FileNotFoundError(f"No XLSX found in {RAW_DIR} — run download_pwc first")

    xlsx_path = xlsx_files[0]
    print(f"Reading: {xlsx_path}")

    df = pd.read_excel(xlsx_path, engine="openpyxl")
    print(f"Loaded {len(df)} rows | columns: {df.columns.tolist()}")

    with duckdb.connect(DUCKDB_PATH) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute("DROP TABLE IF EXISTS raw.raw_pwc_calls")
        con.execute("CREATE TABLE raw.raw_pwc_calls AS SELECT * FROM df")
        row_count = con.execute("SELECT COUNT(*) FROM raw.raw_pwc_calls").fetchone()[0]

    print(f"raw.raw_pwc_calls loaded: {row_count} rows")
