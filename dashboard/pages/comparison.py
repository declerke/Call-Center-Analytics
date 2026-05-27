import os
import dash
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Input, Output

dash.register_page(__name__, path="/comparison", name="Operations Trends", order=1)

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/data/call_center.duckdb")
NO_DATA = html.Div("Pipeline has not run yet.", className="text-warning mt-4 text-center")


def query(sql: str) -> pd.DataFrame:
    try:
        with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
            return con.execute(sql).df()
    except Exception:
        return pd.DataFrame()


layout = dbc.Container([
    html.H2("Operations Trends", className="mt-3 mb-4"),
    html.P("Satvicoder daily operations — 1,251 days of call centre KPIs with 7-day rolling averages.",
           className="text-muted mb-4"),
    dbc.Row([
        dbc.Col(dcc.Graph(id="ops-answer-rate"), md=6),
        dbc.Col(dcc.Graph(id="ops-service-level"), md=6),
    ], className="mb-4"),
    dbc.Row([
        dbc.Col(dcc.Graph(id="ops-volume"), md=6),
        dbc.Col(dcc.Graph(id="ops-abandonment"), md=6),
    ], className="mb-4"),
    dbc.Row([
        dbc.Col(dcc.Graph(id="ops-handle-time"), md=12),
    ]),
    dcc.Interval(id="ops-refresh", interval=60_000, n_intervals=0),
], fluid=True)


@callback(
    Output("ops-answer-rate", "figure"),
    Output("ops-service-level", "figure"),
    Output("ops-volume", "figure"),
    Output("ops-abandonment", "figure"),
    Output("ops-handle-time", "figure"),
    Input("ops-refresh", "n_intervals"),
)
def update(_):
    df = query("SELECT * FROM marts.mart_daily_operations ORDER BY day_index")
    if df.empty:
        empty = px.line(title="No data yet", template="plotly_dark")
        return empty, empty, empty, empty, empty

    fig_answer = go.Figure()
    fig_answer.add_trace(go.Scatter(
        x=df["day_index"], y=df["answer_rate_pct"],
        mode="lines", name="Daily", line={"color": "#636EFA", "width": 1, "dash": "dot"},
        opacity=0.5,
    ))
    fig_answer.add_trace(go.Scatter(
        x=df["day_index"], y=df["answer_rate_7d_avg"],
        mode="lines", name="7-Day Avg", line={"color": "#636EFA", "width": 2},
    ))
    fig_answer.update_layout(
        title="Answer Rate % — Daily vs 7-Day Rolling Average",
        xaxis_title="Day Index", yaxis_title="Answer Rate %",
        template="plotly_dark", legend={"orientation": "h"},
    )

    fig_sla = go.Figure()
    fig_sla.add_trace(go.Scatter(
        x=df["day_index"], y=df["service_level_pct"],
        mode="lines", name="Daily", line={"color": "#00CC96", "width": 1, "dash": "dot"},
        opacity=0.5,
    ))
    fig_sla.add_trace(go.Scatter(
        x=df["day_index"], y=df["service_level_7d_avg"],
        mode="lines", name="7-Day Avg", line={"color": "#00CC96", "width": 2},
    ))
    fig_sla.add_hline(y=80, line_dash="dash", line_color="red",
                      annotation_text="80% SLA Target", annotation_position="top left")
    fig_sla.update_layout(
        title="Service Level % (20s) — Daily vs 7-Day Rolling Average",
        xaxis_title="Day Index", yaxis_title="Service Level %",
        template="plotly_dark", legend={"orientation": "h"},
    )

    fig_vol = px.bar(
        df, x="day_index", y=["answered_calls", "abandoned_calls"],
        title="Daily Call Volume — Answered vs Abandoned",
        labels={"day_index": "Day", "value": "Calls", "variable": "Category"},
        template="plotly_dark",
        barmode="stack",
    )

    fig_aband = go.Figure()
    fig_aband.add_trace(go.Scatter(
        x=df["day_index"], y=df["abandonment_rate_pct"],
        mode="lines", name="Daily", line={"color": "#EF553B", "width": 1, "dash": "dot"},
        opacity=0.5,
    ))
    fig_aband.add_trace(go.Scatter(
        x=df["day_index"], y=df["abandonment_rate_7d_avg"],
        mode="lines", name="7-Day Avg", line={"color": "#EF553B", "width": 2},
    ))
    fig_aband.update_layout(
        title="Abandonment Rate % — Daily vs 7-Day Rolling Average",
        xaxis_title="Day Index", yaxis_title="Abandonment Rate %",
        template="plotly_dark", legend={"orientation": "h"},
    )

    fig_handle = go.Figure()
    fig_handle.add_trace(go.Scatter(
        x=df["day_index"], y=df["avg_answer_speed_secs"],
        mode="lines", name="Avg Answer Speed (s)", line={"color": "#AB63FA"},
    ))
    fig_handle.add_trace(go.Scatter(
        x=df["day_index"], y=df["avg_talk_duration_secs"],
        mode="lines", name="Avg Talk Duration (s)", line={"color": "#FFA15A"},
    ))
    fig_handle.add_trace(go.Scatter(
        x=df["day_index"], y=df["avg_waiting_time_secs"],
        mode="lines", name="Avg Waiting Time (s)", line={"color": "#19D3F3"},
    ))
    fig_handle.update_layout(
        title="Average Handling Times Over Time",
        xaxis_title="Day Index", yaxis_title="Seconds",
        template="plotly_dark", legend={"orientation": "h"},
    )

    return fig_answer, fig_sla, fig_vol, fig_aband, fig_handle
