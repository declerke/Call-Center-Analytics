# Call Center Analytics

> Dual-source call centre intelligence pipeline: Kaggle satvicoder (1,251 daily operational KPI rows) + PWC (5,000 individual call records) → Airflow 3.0 → DuckDB → dbt (10 models, 72 tests) → Plotly Dash (5-page dashboard).

---

## Overview

Two structurally incompatible call centre datasets are ingested from Kaggle, transformed through a layered dbt pipeline, and surfaced in an interactive dark-mode Dash dashboard. The satvicoder dataset provides daily aggregate operational metrics (answer rate, service level, abandonment rate) across 1,251 days. The PWC dataset provides 5,000 individual call records with agent, topic, resolution, and CSAT fields. Each dataset powers dedicated analytics paths that avoid forcing incompatible schemas into a false union.

**Key numbers validated via Airflow DAG run:**

| Dataset | Source | Rows | Schema Type |
|---|---|---|---|
| satvicoder/call-center-data | Kaggle CSV | 1,251 | Daily aggregate KPIs |
| gayatriwagadre/pwc-call-centre-analysis | Kaggle XLSX | 5,000 | Individual call records |

---

## Screenshots

### Executive Overview
![Executive Overview](assets/screenshot_overview.png)

### Operations Trends (Satvicoder — 1,251 days)
![Operations Trends](assets/screenshot_ops_trends.png)

### Agent Leaderboard
![Agent Leaderboard](assets/screenshot_agent.png)

### SLA Monitor
![SLA Monitor](assets/screenshot_sla.png)

### Resolution Funnel
![Resolution Funnel](assets/screenshot_resolution.png)

---

## Data Sources

| Dataset | Owner | Format | Description |
|---|---|---|---|
| call-center-data | satvicoder | CSV | 1,251 daily rows: incoming/answered/abandoned calls, answer rate (%), service level (20s %), avg answer speed, avg talk duration, avg waiting time |
| pwc-call-centre-analysis | gayatriwagadre | XLSX | 5,000 call records: agent, date, time, topic, answered Y/N, resolved Y/N, speed of answer (secs), avg talk duration, satisfaction rating (1–5) |

Both datasets are downloaded at DAG runtime via the Kaggle REST API v1 (`requests.get` with HTTP Basic Auth), bypassing the Kaggle CLI which is incompatible with KGAT-format tokens.

---

## Pipeline Architecture

```
Kaggle REST API
    ├── satvicoder/call-center-data  ──────────────────────┐
    │   (CSV, 1,251 daily aggregate rows)                   │
    └── gayatriwagadre/pwc-call-centre-analysis             │
        (XLSX, 5,000 individual call records)               │
                                                            ▼
                                              DuckDB raw schema
                                       ┌──────────────────────────┐
                                       │ raw.raw_satvicoder_calls  │
                                       │ raw.raw_pwc_calls         │
                                       └──────────────────────────┘
                                                            │
                                              dbt staging (views)
                                       ┌──────────────────────────┐
                                       │ stg_satvicoder_daily      │
                                       │ stg_pwc_calls             │
                                       └──────────────────────────┘
                                                            │
                                           dbt intermediate (views)
                                       ┌──────────────────────────┐
                                       │ int_daily_operations      │
                                       │ int_call_records          │
                                       └──────────────────────────┘
                                                            │
                                              dbt marts (tables)
                     ┌────────────────────────────────────────────────────┐
                     │ mart_daily_operations  mart_agent_performance       │
                     │ mart_sla_compliance    mart_topic_analysis          │
                     │ mart_hourly_patterns   mart_satisfaction_cohorts    │
                     └────────────────────────────────────────────────────┘
                                                            │
                                              Plotly Dash (5 pages)
                              ┌──────────────────────────────────────────┐
                              │ Overview · Operations Trends · Agent     │
                              │ Leaderboard · SLA Monitor · Resolution   │
                              └──────────────────────────────────────────┘
```

**Airflow 3.0 DAG — 9 tasks, all SUCCESS:**

```
[download_satvicoder]  ──┐
                         ├──► ingest_satvicoder ──► ingest_pwc
[download_pwc]         ──┘
                                                        │
                                                    dbt_deps
                                                        │
                                               dbt_run_staging
                                                        │
                                            dbt_run_intermediate
                                                        │
                                               dbt_run_marts
                                                        │
                                                   dbt_test
                                               (72/72 PASS)
```

---

## dbt Layer

### Staging

| Model | Source | Rows | Notes |
|---|---|---|---|
| `stg_satvicoder_daily` | `raw.raw_satvicoder_calls` | 1,251 | HH:MM:SS durations → seconds via `EPOCH(CAST(... AS TIME))`; % columns stripped of any `%` sign |
| `stg_pwc_calls` | `raw.raw_pwc_calls` | 5,000 | Y/N flags → boolean; ISO date cast; TIME cast for durations |

### Intermediate

| Model | Depends On | Rows | Notes |
|---|---|---|---|
| `int_daily_operations` | `stg_satvicoder_daily` | 1,251 | Derives abandonment rate and avg handle time |
| `int_call_records` | `stg_pwc_calls` | 5,000 | Adds `meets_sla_30s` and `total_handle_secs` derived columns |

### Marts (materialized as tables)

| Model | Depends On | Grain | Key Metrics |
|---|---|---|---|
| `mart_daily_operations` | `int_daily_operations` | Day | Answer rate, service level, abandonment rate + 7-day rolling averages |
| `mart_agent_performance` | `int_call_records` | Agent | Total calls, answer rate %, FCR rate %, avg handle time, avg satisfaction |
| `mart_sla_compliance` | `int_call_records` | Date × Hour × Topic | SLA compliance % (30s threshold), met/breach counts |
| `mart_topic_analysis` | `int_call_records` | Topic | Call volume, answer rate, resolution rate, avg satisfaction |
| `mart_hourly_patterns` | `int_call_records` | Hour of day | Call volume, answered calls, SLA compliance, avg satisfaction |
| `mart_satisfaction_cohorts` | `int_call_records` | Band × Topic × Month | Call counts by satisfaction band (Promoter/Satisfied/Neutral/Dissatisfied/Detractor/Not Rated) |

**dbt test results: 72/72 PASS**

---

## Dashboard

Built with Plotly Dash 2.18.2, Dash Bootstrap Components (DARKLY theme), multi-page with `use_pages=True`.

| Page | Path | Data Source | Charts |
|---|---|---|---|
| Executive Overview | `/` | agent_performance, topic_analysis, sla_compliance, satisfaction_cohorts | 4 KPI cards, calls-by-centre bar, daily volume line, topic pie, satisfaction distribution |
| Operations Trends | `/comparison` | mart_daily_operations | Answer rate 7d rolling avg, service level 7d avg, call volume stacked bar, abandonment rate 7d avg, handling times |
| Agent Leaderboard | `/agent` | mart_agent_performance | Top-10 satisfaction bar, handle time vs satisfaction scatter, filterable data table |
| SLA Monitor | `/sla` | mart_sla_compliance | Daily SLA % line (80% target line), SLA by topic, hourly volume heatmap, centre summary |
| Resolution Funnel | `/resolution` | mart_topic_analysis, mart_satisfaction_cohorts, mart_daily_operations | Satvicoder operational summary (92.7% answer rate, 70.9% service level), PWC funnel (5,000 → 4,054 answered → 3,646 resolved), resolution by topic, CSAT cohorts, monthly positive CSAT trend |

All pages use read-only DuckDB connections per callback. The `duckdb_pool` Airflow pool (1 slot) serialises all write operations.

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| **Separate dbt paths per dataset** | Satvicoder (daily aggregates) and PWC (individual records) have incompatible schemas — forcing a UNION would require fabricating missing columns. Separate staging/intermediate models preserve data fidelity and allow each dataset to express its natural grain. |
| **Kaggle REST API instead of CLI** | Kaggle CLI 1.7.4 is incompatible with KGAT-format tokens (protobuf `DatasetInfo` schema mismatch, `KeyError: 'username'`). Direct HTTP calls to `api/v1/datasets/download` with Basic Auth work reliably with any token format. |
| **DuckDB single-writer pool** | DuckDB has a single-writer constraint. The Airflow `duckdb_pool` (1 slot) enforces write serialisation across ingest and dbt tasks without any application-level locking code. |
| **dbt `generate_schema_name` macro** | Without the macro, dbt-duckdb prefixes all schema names with the target schema (e.g. `main.main_staging`). The macro overrides this to produce clean schema names: `raw`, `staging`, `intermediate`, `marts`. |
| **7-day rolling averages in mart_daily_operations** | Raw daily KPIs are noisy. Window functions (`ROWS BETWEEN 6 PRECEDING AND CURRENT ROW`) smooth the signal for trend analysis without losing the raw daily values. |
| **COALESCE on sla_compliance_pct** | Date/hour/topic buckets with zero answered calls produce `NULL` via `NULLIF(0,0)`. `COALESCE(..., 0)` keeps the column non-null, allowing `not_null` tests to pass and avoiding NaN artifacts in the dashboard. |

---

## Call Centre Domain Reference

| Term | Definition |
|---|---|
| Answer Rate % | Answered calls ÷ incoming calls × 100 |
| Service Level (20s) % | Calls answered within 20 seconds ÷ answered calls × 100 |
| SLA Compliance (30s) | Industry standard: calls answered within 30 seconds = compliant |
| FCR (First Contact Resolution) | Resolved calls ÷ answered calls × 100 — measures issue resolution without escalation |
| Abandonment Rate % | Abandoned calls ÷ incoming calls × 100 |
| Avg Handle Time | Avg speed of answer + avg talk duration (seconds) |
| CSAT Bands | Promoter (5), Satisfied (4), Neutral (3), Dissatisfied (2), Detractor (1), Not Rated |

---

## Project Structure

```
call-center-analytics/
├── Dockerfile.airflow          # Airflow 3.0 image with dbt-duckdb, openpyxl, requests
├── docker-compose.yml          # 7 services: postgres, airflow-init, api-server, scheduler,
│                               #   dag-processor, triggerer, dash
├── .env.example                # Template — copy to .env and fill credentials
├── requirements.txt            # Python deps for local testing
├── dags/
│   └── call_center_dag.py      # 9-task DAG: download → ingest → dbt deps/staging/
│                               #   intermediate/marts/test
├── ingestion/
│   ├── ingest_satvicoder.py    # CSV → DuckDB raw.raw_satvicoder_calls
│   └── ingest_pwc.py           # XLSX → DuckDB raw.raw_pwc_calls
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── models/
│       ├── staging/            # stg_satvicoder_daily, stg_pwc_calls
│       ├── intermediate/       # int_daily_operations, int_call_records
│       └── marts/              # 6 mart tables
├── dashboard/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── app.py                  # Dash multi-page app (port 8050)
│   └── pages/
│       ├── overview.py
│       ├── comparison.py       # Operations Trends (satvicoder daily KPIs)
│       ├── agent.py
│       ├── sla.py
│       └── resolution.py
├── tests/
│   ├── test_ingestion.py       # 9 pytest tests — row counts, column presence
│   └── test_transformation.py  # 14 pytest tests — mart ranges, row counts
└── assets/
    └── screenshot_*.png
```

---

## Quick Start

### Prerequisites
- Docker Desktop (with Linux containers)
- Kaggle account — username and API token from **kaggle.com → Settings → API → Create New Token**

### Setup

```bash
git clone https://github.com/declerke/Call-Center-Analytics.git
cd Call-Center-Analytics
cp .env.example .env
# Edit .env: set KAGGLE_USERNAME and KAGGLE_KEY
```

### Run

```bash
docker compose up --build
```

- Airflow UI: http://localhost:8080 (admin / Admin)
- Dashboard: http://localhost:8050

Trigger the `call_center_pipeline` DAG from the Airflow UI. The full run takes under 3 minutes.

### Run Tests (after DAG completes)

```bash
# From inside the scheduler container:
docker compose exec airflow-scheduler bash -c \
  "cd /opt/airflow && DUCKDB_PATH=/opt/airflow/data/call_center.duckdb pytest tests/ -v"
```

---

## Skills Demonstrated

| Category | Technologies |
|---|---|
| **Orchestration** | Apache Airflow 3.0 (AIP-72 architecture), LocalExecutor, custom Airflow pools |
| **Storage** | DuckDB 1.2.2, schema-per-layer design (raw / staging / intermediate / marts) |
| **Transformation** | dbt-duckdb 1.9.2, 10 models across 3 layers, 72 schema tests, window functions |
| **Ingestion** | Kaggle REST API v1 (bypassing CLI), openpyxl for XLSX, pandas for CSV |
| **Visualisation** | Plotly Dash 2.18.2, Dash Bootstrap Components, multi-page app, `use_pages=True` |
| **Containerisation** | Docker Compose, 7-service architecture, named volumes for data persistence |
| **Testing** | pytest (ingestion + transformation), dbt schema tests (not_null, unique, accepted_values) |
| **Engineering Patterns** | Separate analytics paths for incompatible schemas, rolling window aggregations, COALESCE for null-safe KPIs |

---

## License

MIT
