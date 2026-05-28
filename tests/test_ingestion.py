"""
Ingestion tests — verify raw tables exist in DuckDB with correct row counts and columns.
Run after the Airflow DAG's ingest_satvicoder and ingest_pwc tasks complete.
"""

import pytest

SATVICODER_EXPECTED_COLS = {
    "Index", "Incoming Calls", "Answered Calls", "Answer Rate",
    "Abandoned Calls", "Answer Speed (AVG)", "Talk Duration (AVG)",
    "Waiting Time (AVG)", "Service Level (20 Seconds)",
}

PWC_EXPECTED_COLS = {
    "Call Id", "Agent", "Date", "Time", "Topic",
    "Answered (Y/N)", "Resolved", "Speed of answer in seconds",
    "AvgTalkDuration", "Satisfaction rating",
}


def test_raw_satvicoder_table_exists(con):
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'"
    ).fetchall()
    names = {r[0] for r in tables}
    assert "raw_satvicoder_calls" in names


def test_raw_pwc_table_exists(con):
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw'"
    ).fetchall()
    names = {r[0] for r in tables}
    assert "raw_pwc_calls" in names


def test_satvicoder_row_count(con):
    count = con.execute("SELECT COUNT(*) FROM raw.raw_satvicoder_calls").fetchone()[0]
    assert count == 1251, f"Expected 1,251 satvicoder daily rows, got {count}"


def test_pwc_row_count(con):
    count = con.execute("SELECT COUNT(*) FROM raw.raw_pwc_calls").fetchone()[0]
    assert count == 5000, f"Expected 5,000 PWC rows, got {count}"


def test_satvicoder_columns(con):
    cols = {r[0] for r in con.execute("DESCRIBE raw.raw_satvicoder_calls").fetchall()}
    assert SATVICODER_EXPECTED_COLS.issubset(cols), (
        f"Missing satvicoder columns: {SATVICODER_EXPECTED_COLS - cols}"
    )


def test_pwc_columns(con):
    cols = {r[0] for r in con.execute("DESCRIBE raw.raw_pwc_calls").fetchall()}
    assert PWC_EXPECTED_COLS.issubset(cols), (
        f"Missing PWC columns: {PWC_EXPECTED_COLS - cols}"
    )


def test_pwc_satisfaction_range(con):
    result = con.execute("""
        SELECT MIN("Satisfaction rating"), MAX("Satisfaction rating")
        FROM raw.raw_pwc_calls
        WHERE "Satisfaction rating" IS NOT NULL
    """).fetchone()
    assert result[0] >= 1, f"Min satisfaction below 1: {result[0]}"
    assert result[1] <= 5, f"Max satisfaction above 5: {result[1]}"


def test_pwc_answered_values(con):
    values = {r[0] for r in con.execute(
        "SELECT DISTINCT \"Answered (Y/N)\" FROM raw.raw_pwc_calls"
    ).fetchall()}
    assert values.issubset({"Y", "N"}), f"Unexpected answered values: {values}"


def test_satvicoder_incoming_calls_positive(con):
    negatives = con.execute("""
        SELECT COUNT(*) FROM raw.raw_satvicoder_calls
        WHERE "Incoming Calls" IS NOT NULL AND CAST("Incoming Calls" AS INTEGER) < 0
    """).fetchone()[0]
    assert negatives == 0, f"Found {negatives} rows with negative incoming calls"
