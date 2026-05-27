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


def build_funnel(df: pd.DataFrame, centre: str, title: str):
    sub = df[df["source_centre"] == centre]
    if sub.empty:
        return px.bar(title=f"{title} — no data", template="plotly_dark")
    total = int(sub["total_calls"].sum())
    answered = int(sub["answered_calls"].sum())
    resolved = int(sub["resolved_calls"].sum())
    fig = go.Figure(go.Funnel(
        y=["Total Calls", "Answered", "Resolved"],
        x=[total, answered, resolved],
        textinfo="value+percent initial",
    ))
    fig.update_layout(title=title, template="plotly_dark")
    return fig


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

    if df_topic.empty:
        empty = px.bar(title="No data yet", template="plotly_dark")
        return empty, empty, empty, empty, empty

    fig_sat_funnel = build_funnel(df_topic, "satvicoder", "Satvicoder Centre — Call Funnel")
    fig_pwc_funnel = build_funnel(df_topic, "pwc", "PWC Centre — Call Funnel")

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
