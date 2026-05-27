"""
Transformation tests — verify dbt mart tables in DuckDB after dbt run completes.
Run after the Airflow DAG's dbt_run_marts task.
"""

import os
import duckdb
import pytest

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/data/call_center.duckdb")

VALID_SATISFACTION_BANDS = {
    "Promoter (5)", "Satisfied (4)", "Neutral (3)",
    "Dissatisfied (2)", "Detractor (1)", "Not Rated",
}


@pytest.fixture(scope="module")
def con():
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    yield conn
    conn.close()


def test_unified_calls_total_rows(con):
    count = con.execute("SELECT COUNT(*) FROM intermediate.int_unified_calls").fetchone()[0]
    assert count == 10000, f"Expected 10,000 unified rows (5,000 × 2), got {count}"


def test_unified_calls_source_centres(con):
    centres = {r[0] for r in con.execute(
        "SELECT DISTINCT source_centre FROM intermediate.int_unified_calls"
    ).fetchall()}
    assert centres == {"satvicoder", "pwc"}, f"Unexpected source centres: {centres}"


def test_agent_performance_no_null_agents(con):
    nulls = con.execute(
        "SELECT COUNT(*) FROM marts.mart_agent_performance WHERE agent IS NULL"
    ).fetchone()[0]
    assert nulls == 0, f"Found {nulls} null agents in mart_agent_performance"


def test_agent_performance_answer_rate_range(con):
    result = con.execute("""
        SELECT MIN(answer_rate_pct), MAX(answer_rate_pct)
        FROM marts.mart_agent_performance
        WHERE answer_rate_pct IS NOT NULL
    """).fetchone()
    assert result[0] >= 0, f"Answer rate below 0: {result[0]}"
    assert result[1] <= 100, f"Answer rate above 100: {result[1]}"


def test_sla_compliance_pct_range(con):
    result = con.execute("""
        SELECT MIN(sla_compliance_pct), MAX(sla_compliance_pct)
        FROM marts.mart_sla_compliance
        WHERE sla_compliance_pct IS NOT NULL
    """).fetchone()
    assert result[0] >= 0, f"SLA compliance below 0: {result[0]}"
    assert result[1] <= 100, f"SLA compliance above 100: {result[1]}"


def test_topic_analysis_resolution_rate_range(con):
    result = con.execute("""
        SELECT MIN(resolution_rate_pct), MAX(resolution_rate_pct)
        FROM marts.mart_topic_analysis
        WHERE resolution_rate_pct IS NOT NULL
    """).fetchone()
    assert result[0] >= 0, f"Resolution rate below 0: {result[0]}"
    assert result[1] <= 100, f"Resolution rate above 100: {result[1]}"


def test_hourly_patterns_valid_hours(con):
    invalid = con.execute("""
        SELECT COUNT(*) FROM marts.mart_hourly_patterns
        WHERE call_hour < 0 OR call_hour > 23
    """).fetchone()[0]
    assert invalid == 0, f"Found {invalid} invalid call hours"


def test_satisfaction_cohorts_valid_bands(con):
    bands = {r[0] for r in con.execute(
        "SELECT DISTINCT satisfaction_band FROM marts.mart_satisfaction_cohorts"
    ).fetchall()}
    invalid = bands - VALID_SATISFACTION_BANDS
    assert not invalid, f"Unknown satisfaction bands: {invalid}"


def test_topic_analysis_both_centres(con):
    centres = {r[0] for r in con.execute(
        "SELECT DISTINCT source_centre FROM marts.mart_topic_analysis"
    ).fetchall()}
    assert centres == {"satvicoder", "pwc"}, f"Missing centres in topic analysis: {centres}"


def test_agent_performance_fcr_rate_non_negative(con):
    neg = con.execute("""
        SELECT COUNT(*) FROM marts.mart_agent_performance
        WHERE fcr_rate_pct < 0
    """).fetchone()[0]
    assert neg == 0, f"Found {neg} rows with negative FCR rate"
