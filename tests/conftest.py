import os
import pytest
import duckdb

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/data/call_center.duckdb")


@pytest.fixture(scope="module")
def con():
    with duckdb.connect(DUCKDB_PATH, read_only=True) as c:
        yield c
