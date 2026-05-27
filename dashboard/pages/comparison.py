import os
import dash
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Input, Output, dash_table

dash.register_page(__name__, path="/comparison", name="Centre Comparison", order=1)

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/data/call_center.duckdb")
NO_DATA = html.Div("Pipeline has not run yet.", className="text-warning mt-4 text-center")


def query(sql: str) -> pd.DataFrame:
    try:
        with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
            return con.execute(sql).df()
    except Exception:
        return pd.DataFrame()


layout = dbc.Container([
    html.H2("Centre Comparison", className="mt-3 mb-4"),
    dbc.Row([
        dbc.Col(dcc.Graph(id="comp-bar"), md=8),
        dbc.Col(dcc.Graph(id="comp-radar"), md=4),
    ], className="mb-4"),
    dbc.Row([
        dbc.Col(dcc.Graph(id="comp-fcr"), md=6),
        dbc.Col(dcc.Graph(id="comp-sat"), md=6),
    ], className="mb-4"),
    html.H5("Centre-Level Summary", className="mt-3 mb-2"),
    html.Div(id="comp-table"),
    dcc.Interval(id="comp-refresh", interval=60_000, n_intervals=0),
], fluid=True)


@callback(
    Output("comp-bar", "figure"),
    Output("comp-radar", "figure"),
    Output("comp-fcr", "figure"),
    Output("comp-sat", "figure"),
    Output("comp-table", "children"),
    Input("comp-refresh", "n_intervals"),
)
def update(_):
    df = query("SELECT * FROM marts.mart_agent_performance")
    if df.empty:
        empty = px.bar(title="No data yet", template="plotly_dark")
        return empty, empty, empty, empty, NO_DATA

    summary = df.groupby("source_centre", as_index=False).agg(
        total_calls=("total_calls", "sum"),
        answer_rate_pct=("answer_rate_pct", "mean"),
        fcr_rate_pct=("fcr_rate_pct", "mean"),
        avg_satisfaction=("avg_satisfaction", "mean"),
        avg_speed_of_answer_secs=("avg_speed_of_answer_secs", "mean"),
    ).round(2)

    melted = summary.melt(
        id_vars="source_centre",
        value_vars=["answer_rate_pct", "fcr_rate_pct", "avg_satisfaction"],
        var_name="Metric", value_name="Value",
    )
    metric_labels = {
        "answer_rate_pct": "Answer Rate %",
        "fcr_rate_pct": "FCR Rate %",
        "avg_satisfaction": "Avg Satisfaction",
    }
    melted["Metric"] = melted["Metric"].map(metric_labels)

    fig_bar = px.bar(
        melted, x="Metric", y="Value", color="source_centre", barmode="group",
        title="Key KPIs — Centre Comparison",
        labels={"source_centre": "Centre", "Value": "Value"},
        template="plotly_dark",
    )

    categories = ["Answer Rate %", "FCR Rate %", "Avg Sat ×20", "Speed Score"]
    fig_radar = go.Figure()
    for _, row in summary.iterrows():
        fig_radar.add_trace(go.Scatterpolar(
            r=[
                row["answer_rate_pct"],
                row["fcr_rate_pct"],
                row["avg_satisfaction"] * 20,
                max(0, 100 - row["avg_speed_of_answer_secs"]),
            ],
            theta=categories,
            fill="toself",
            name=row["source_centre"],
        ))
    fig_radar.update_layout(
        polar={"radialaxis": {"visible": True, "range": [0, 100]}},
        title="Performance Radar",
        template="plotly_dark",
    )

    fig_fcr = px.bar(
        summary, x="source_centre", y="fcr_rate_pct", color="source_centre",
        title="First-Contact Resolution Rate by Centre",
        labels={"source_centre": "Centre", "fcr_rate_pct": "FCR Rate %"},
        template="plotly_dark",
    )

    fig_sat = px.bar(
        summary, x="source_centre", y="avg_satisfaction", color="source_centre",
        title="Avg Satisfaction Score by Centre",
        labels={"source_centre": "Centre", "avg_satisfaction": "Avg Satisfaction"},
        template="plotly_dark",
    )

    table = dash_table.DataTable(
        data=summary.to_dict("records"),
        columns=[{"name": c, "id": c} for c in summary.columns],
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": "#303030", "color": "white", "fontWeight": "bold"},
        style_cell={"backgroundColor": "#1e1e1e", "color": "white", "textAlign": "center"},
        page_size=10,
    )

    return fig_bar, fig_radar, fig_fcr, fig_sat, table
