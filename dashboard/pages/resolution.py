import os
import dash
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Input, Output

dash.register_page(__name__, path="/resolution", name="Resolution Funnel", order=4)

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/data/call_center.duckdb")
NO_DATA = html.Div("Pipeline has not run yet.", className="text-warning mt-4 text-center")


def query(sql: str) -> pd.DataFrame:
    try:
        with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
            return con.execute(sql).df()
    except Exception:
        return pd.DataFrame()


layout = dbc.Container([
    html.H2("Resolution Funnel", className="mt-3 mb-4"),
    dbc.Row([
        dbc.Col(dcc.Graph(id="res-funnel-sat"), md=6),
        dbc.Col(dcc.Graph(id="res-funnel-pwc"), md=6),
    ], className="mb-4"),
    dbc.Row([
        dbc.Col(dcc.Graph(id="res-topic-bar"), md=6),
        dbc.Col(dcc.Graph(id="res-sat-cohort"), md=6),
    ], className="mb-4"),
    dbc.Row([
        dbc.Col(dcc.Graph(id="res-monthly-trend"), md=12),
    ]),
    dcc.Interval(id="res-refresh", interval=60_000, n_intervals=0),
], fluid=True)


@callback(
    Output("res-funnel-sat", "figure"),
    Output("res-funnel-pwc", "figure"),
    Output("res-topic-bar", "figure"),
    Output("res-sat-cohort", "figure"),
    Output("res-monthly-trend", "figure"),
    Input("res-refresh", "n_intervals"),
)
def update(_):
    df_topic = query("SELECT * FROM marts.mart_topic_analysis")
    df_cohort = query("SELECT * FROM marts.mart_satisfaction_cohorts")
    df_ops = query("""
        SELECT
            ROUND(AVG(answer_rate_pct), 1)        AS avg_answer_rate,
            ROUND(AVG(service_level_pct), 1)      AS avg_service_level,
            ROUND(AVG(abandonment_rate_pct), 1)   AS avg_abandonment,
            SUM(incoming_calls)                    AS total_incoming,
            SUM(answered_calls)                    AS total_answered,
            SUM(abandoned_calls)                   AS total_abandoned
        FROM marts.mart_daily_operations
    """)

    if df_topic.empty:
        empty = px.bar(title="No data yet", template="plotly_dark")
        return empty, empty, empty, empty, empty

    # Satvicoder operational summary (indicator panel)
    if not df_ops.empty and df_ops["total_incoming"].iloc[0]:
        row = df_ops.iloc[0]
        fig_sat_funnel = go.Figure()
        fig_sat_funnel.add_trace(go.Indicator(
            mode="number+delta",
            value=float(row["avg_answer_rate"]),
            title={"text": "Avg Answer Rate %<br><span style='font-size:0.8em'>1,251 operational days</span>"},
            domain={"x": [0, 0.5], "y": [0.6, 1]},
            number={"suffix": "%"},
        ))
        fig_sat_funnel.add_trace(go.Indicator(
            mode="number",
            value=float(row["avg_service_level"]),
            title={"text": "Avg Service Level % (20s)"},
            domain={"x": [0.5, 1], "y": [0.6, 1]},
            number={"suffix": "%"},
        ))
        fig_sat_funnel.add_trace(go.Indicator(
            mode="number",
            value=float(row["avg_abandonment"]),
            title={"text": "Avg Abandonment Rate %"},
            domain={"x": [0, 0.5], "y": [0.1, 0.5]},
            number={"suffix": "%"},
        ))
        fig_sat_funnel.add_trace(go.Indicator(
            mode="number",
            value=int(row["total_incoming"]),
            title={"text": "Total Incoming Calls"},
            domain={"x": [0.5, 1], "y": [0.1, 0.5]},
            number={"valueformat": ","},
        ))
        fig_sat_funnel.update_layout(
            title="Satvicoder Centre — Operational Summary",
            template="plotly_dark",
        )
    else:
        fig_sat_funnel = px.bar(title="Satvicoder data unavailable", template="plotly_dark")

    # PWC call funnel
    pwc = df_topic[df_topic["source_centre"] == "pwc"]
    if not pwc.empty:
        total = int(pwc["total_calls"].sum())
        answered = int(pwc["answered_calls"].sum())
        resolved = int(pwc["resolved_calls"].sum())
        fig_pwc_funnel = go.Figure(go.Funnel(
            y=["Total Calls", "Answered", "Resolved"],
            x=[total, answered, resolved],
            textinfo="value+percent initial",
        ))
        fig_pwc_funnel.update_layout(title="PWC Centre — Call Funnel", template="plotly_dark")
    else:
        fig_pwc_funnel = px.bar(title="PWC data unavailable", template="plotly_dark")

    fig_topic_bar = px.bar(
        df_topic, x="topic", y="resolution_rate_pct", color="source_centre", barmode="group",
        title="Resolution Rate by Topic",
        labels={"topic": "Topic", "resolution_rate_pct": "Resolution Rate %", "source_centre": "Centre"},
        template="plotly_dark",
    )

    if not df_cohort.empty:
        order = ["Promoter (5)", "Satisfied (4)", "Neutral (3)", "Dissatisfied (2)", "Detractor (1)", "Not Rated"]
        cohort_agg = df_cohort.groupby(["satisfaction_band", "source_centre"], as_index=False)["call_count"].sum()
        fig_cohort = px.bar(
            cohort_agg, x="satisfaction_band", y="call_count", color="source_centre",
            barmode="group", category_orders={"satisfaction_band": order},
            title="Satisfaction Cohort Distribution",
            labels={"satisfaction_band": "Satisfaction Band", "call_count": "Calls", "source_centre": "Centre"},
            template="plotly_dark",
        )

        monthly = df_cohort[df_cohort["satisfaction_band"].isin(["Promoter (5)", "Satisfied (4)"])]
        monthly_agg = monthly.groupby(["call_month", "source_centre"], as_index=False)["call_count"].sum()
        fig_monthly = px.line(
            monthly_agg.sort_values("call_month"), x="call_month", y="call_count", color="source_centre",
            title="Positive Satisfaction Trend by Month (Scores 4+5)",
            labels={"call_month": "Month", "call_count": "Calls", "source_centre": "Centre"},
            template="plotly_dark",
            markers=True,
        )
    else:
        fig_cohort = px.bar(title="Cohort data unavailable", template="plotly_dark")
        fig_monthly = px.line(title="Trend data unavailable", template="plotly_dark")

    return fig_sat_funnel, fig_pwc_funnel, fig_topic_bar, fig_cohort, fig_monthly
