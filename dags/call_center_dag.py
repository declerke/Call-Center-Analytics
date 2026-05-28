"""
Call Center Analytics Pipeline
Downloads satvicoder + PWC call center datasets from Kaggle → ingests to DuckDB
→ dbt staging / intermediate / marts → 54 schema tests.
Schedule: @weekly
"""

import sys
import os
import io
import zipfile
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

sys.path.insert(0, "/opt/airflow")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "depends_on_past": False,
}

DBT_DIR = "/opt/airflow/dbt"
POOL = "duckdb_pool"


def _download_dataset(owner: str, dataset: str, dest_dir: str) -> None:
    """Download a Kaggle dataset via REST API — bypasses CLI protobuf issues with KGAT tokens."""
    os.makedirs(dest_dir, exist_ok=True)
    username = os.getenv("KAGGLE_USERNAME", "")
    key = os.getenv("KAGGLE_KEY", "")

    url = f"https://www.kaggle.com/api/v1/datasets/download/{owner}/{dataset}"
    print(f"Downloading {url}")

    resp = requests.get(url, auth=(username, key), stream=True, allow_redirects=True, timeout=300)

    if resp.status_code != 200:
        raise RuntimeError(f"Kaggle API returned HTTP {resp.status_code} for {owner}/{dataset}.\n{resp.text[:500]}")

    content = b"".join(resp.iter_content(chunk_size=65536))
    print(f"Downloaded {len(content) / 1024:.1f} KB")

    with zipfile.ZipFile(io.BytesIO(content)) as z:
        z.extractall(dest_dir)
    print(f"Extracted to {dest_dir}: {os.listdir(dest_dir)}")


def _download_satvicoder():
    _download_dataset(
        owner="satvicoder",
        dataset="call-center-data",
        dest_dir="/opt/airflow/data/raw/satvicoder",
    )


def _download_pwc():
    _download_dataset(
        owner="gayatriwagadre",
        dataset="pwc-call-centre-analysis",
        dest_dir="/opt/airflow/data/raw/pwc",
    )


def _ingest_satvicoder():
    from ingestion.ingest_satvicoder import run
    run()


def _ingest_pwc():
    from ingestion.ingest_pwc import run
    run()


with DAG(
    dag_id="call_center_pipeline",
    default_args=default_args,
    description="Call centre analytics — satvicoder + PWC Kaggle datasets → dbt → DuckDB",
    schedule="@weekly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["call-center", "analytics", "dbt", "duckdb", "kaggle"],
) as dag:

    download_satvicoder = PythonOperator(
        task_id="download_satvicoder",
        python_callable=_download_satvicoder,
    )

    download_pwc = PythonOperator(
        task_id="download_pwc",
        python_callable=_download_pwc,
    )

    ingest_satvicoder = PythonOperator(
        task_id="ingest_satvicoder",
        python_callable=_ingest_satvicoder,
        pool=POOL,
    )

    ingest_pwc = PythonOperator(
        task_id="ingest_pwc",
        python_callable=_ingest_pwc,
        pool=POOL,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps --profiles-dir {DBT_DIR}",
        pool=POOL,
    )

    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --select staging --profiles-dir {DBT_DIR}",
        pool=POOL,
    )

    dbt_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=f"cd {DBT_DIR} && dbt run --select intermediate --profiles-dir {DBT_DIR}",
        pool=POOL,
    )

    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --select marts --profiles-dir {DBT_DIR}",
        pool=POOL,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {DBT_DIR}",
        pool=POOL,
    )

    # Downloads run in parallel; ingests serialized via duckdb_pool
    download_satvicoder >> ingest_satvicoder
    download_pwc >> ingest_pwc
    ingest_satvicoder >> ingest_pwc
    ingest_pwc >> dbt_deps >> dbt_staging >> dbt_intermediate >> dbt_marts >> dbt_test
