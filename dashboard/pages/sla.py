import os
import dash
import duckdb
import pandas as pd
import plotly.express as px
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Input, Output

dash.register_page(__name__, path="/sla", name="SLA Monitor", order=3)

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/data/call_center.duckdb")
NO_DATA = html.Div("Pipeline has not run yet.", className="text-warning mt-4 text-center")


def query(sql: str) -> pd.DataFrame:
    try:
        with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
            return con.execute(sql).df()
    except Exception:
        return pd.DataFrame()


layout = dbc.Container([
    html.H2("SLA Monitor", className="mt-3 mb-4"),
    html.P("SLA threshold: calls answered within 30 seconds = compliant (industry standard).",
           className="text-muted mb-4"),
    dbc.Row([
        dbc.Col(dcc.Graph(id="sla-daily-line"), md=12),
    ], className="mb-4"),
    dbc.Row([
        dbc.Col(dcc.Graph(id="sla-topic-bar"), md=6),
        dbc.Col(dcc.Graph(id="sla-hourly-heatmap"), md=6),
    ], className="mb-4"),
    dbc.Row([
        dbc.Col(dcc.Graph(id="sla-centre-compare"), md=6),
    ]),
    dcc.Interval(id="sla-refresh", interval=60_000, n_intervals=0),
], fluid=True)


@callback(
    Output("sla-daily-line", "figure"),
    Output("sla-topic-bar", "figure"),
    Output("sla-hourly-heatmap", "figure"),
    Output("sla-centre-compare", "figure"),
    Input("sla-refresh", "n_intervals"),
)
def update(_):
    df = query("SELECT * FROM marts.mart_sla_compliance")
    if df.empty:
        empty = px.bar(title="No data yet", template="plotly_dark")
        return empty, empty, empty, empty

    daily = (df.groupby(["call_date", "source_centre"], as_index=False)
               .agg(sla_met=("sla_met_calls", "sum"), answered=("answered_calls", "sum"))
               .assign(sla_pct=lambda x: (x["sla_met"] / x["answered"].replace(0, float("nan")) * 100).round(2))
               .dropna(subset=["sla_pct"])
               .sort_values("call_date"))

    fig_daily = px.line(
        daily, x="call_date", y="sla_pct", color="source_centre",
        title="Daily SLA Compliance % by Centre",
        labels={"call_date": "Date", "sla_pct": "SLA Compliance %", "source_centre": "Centre"},
        template="plotly_dark",
    )
    fig_daily.add_hline(y=80, line_dash="dash", line_color="red",
                        annotation_text="80% Target", annotation_position="top left")

    topic_df = (df.groupby(["topic", "source_centre"], as_index=False)
                  .agg(sla_met=("sla_met_calls", "sum"), answered=("answered_calls", "sum"))
                  .assign(sla_pct=lambda x: (x["sla_met"] / x["answered"].replace(0, float("nan")) * 100).round(2))
                  .dropna(subset=["sla_pct"]))

    fig_topic = px.bar(
        topic_df, x="topic", y="sla_pct", color="source_centre", barmode="group",
        title="SLA Compliance % by Topic",
        labels={"topic": "Topic", "sla_pct": "SLA %", "source_centre": "Centre"},
        template="plotly_dark",
    )

    hourly = (df.groupby(["call_hour", "source_centre"], as_index=False)
                .agg(total=("total_calls", "sum"))
                .pivot(index="call_hour", columns="source_centre", values="total")
                .fillna(0)
                .reset_index())

    if len(hourly.columns) > 1:
        fig_hourly = px.imshow(
            hourly.set_index("call_hour").T,
            title="Call Volume Heatmap by Hour of Day",
            labels={"x": "Hour", "y": "Centre", "color": "Calls"},
            template="plotly_dark",
            color_continuous_scale="Blues",
        )
    else:
        fig_hourly = px.bar(title="Heatmap unavailable", template="plotly_dark")

    centre_df = (df.groupby("source_centre", as_index=False)
                   .agg(sla_met=("sla_met_calls", "sum"), answered=("answered_calls", "sum"))
                   .assign(sla_pct=lambda x: (x["sla_met"] / x["answered"].replace(0, float("nan")) * 100).round(2))
                   .dropna(subset=["sla_pct"]))

    fig_centre = px.bar(
        centre_df, x="source_centre", y="sla_pct", color="source_centre",
        title="Overall SLA Compliance by Centre",
        labels={"source_centre": "Centre", "sla_pct": "SLA Compliance %"},
        template="plotly_dark",
    )
    fig_centre.add_hline(y=80, line_dash="dash", line_color="red",
                         annotation_text="80% Target")

    return fig_daily, fig_topic, fig_hourly, fig_centre
