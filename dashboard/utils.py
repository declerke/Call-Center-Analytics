import os
import logging
import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/data/call_center.duckdb")


def query(sql: str) -> pd.DataFrame:
    try:
        with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
            return con.execute(sql).df()
    except Exception as e:
        logger.exception("DuckDB query failed: %s", e)
        return pd.DataFrame()
