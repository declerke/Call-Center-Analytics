import os
import dash
import duckdb
import pandas as pd
import plotly.express as px
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Input, Output, dash_table

dash.register_page(__name__, path="/agent", name="Agent Leaderboard", order=2)

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/data/call_center.duckdb")
NO_DATA = html.Div("Pipeline has not run yet.", className="text-warning mt-4 text-center")


def query(sql: str) -> pd.DataFrame:
    try:
        with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
            return con.execute(sql).df()
    except Exception:
        return pd.DataFrame()


layout = dbc.Container([
    html.H2("Agent Leaderboard", className="mt-3 mb-4"),
    dbc.Row([
        dbc.Col([
            html.Label("Filter by Centre:"),
            dcc.Dropdown(
                id="agent-centre-filter",
                options=[
                    {"label": "All", "value": "all"},
                    {"label": "Satvicoder", "value": "satvicoder"},
                    {"label": "PWC", "value": "pwc"},
                ],
                value="all",
                clearable=False,
                style={"color": "#000"},
            ),
        ], md=3),
    ], className="mb-4"),
    dbc.Row([
        dbc.Col(dcc.Graph(id="agent-sat-bar"), md=6),
        dbc.Col(dcc.Graph(id="agent-scatter"), md=6),
    ], className="mb-4"),
    html.H5("Agent Rankings — Full Table", className="mt-3 mb-2"),
    html.Div(id="agent-table"),
    dcc.Interval(id="agent-refresh", interval=60_000, n_intervals=0),
], fluid=True)


@callback(
    Output("agent-sat-bar", "figure"),
    Output("agent-scatter", "figure"),
    Output("agent-table", "children"),
    Input("agent-centre-filter", "value"),
    Input("agent-refresh", "n_intervals"),
)
def update(centre, _):
    df = query("SELECT * FROM marts.mart_agent_performance")
    if df.empty:
        empty = px.bar(title="No data yet", template="plotly_dark")
        return empty, empty, NO_DATA

    if centre != "all":
        df = df[df["source_centre"] == centre]

    top10 = df.nlargest(10, "avg_satisfaction")
    fig_sat = px.bar(
        top10.sort_values("avg_satisfaction"),
        x="avg_satisfaction", y="agent", orientation="h",
        color="source_centre",
        title="Top 10 Agents by Satisfaction Score",
        labels={"avg_satisfaction": "Avg Satisfaction", "agent": "Agent", "source_centre": "Centre"},
        template="plotly_dark",
    )

    fig_scatter = px.scatter(
        df.dropna(subset=["avg_handle_time_secs", "avg_satisfaction"]),
        x="avg_handle_time_secs", y="avg_satisfaction",
        color="source_centre", hover_data=["agent"],
        title="Handle Time vs Satisfaction",
        labels={
            "avg_handle_time_secs": "Avg Handle Time (secs)",
            "avg_satisfaction": "Avg Satisfaction",
            "source_centre": "Centre",
        },
        template="plotly_dark",
    )

    display_cols = ["agent", "source_centre", "total_calls", "answer_rate_pct",
                    "fcr_rate_pct", "avg_handle_time_secs", "avg_satisfaction"]
    display_df = df[display_cols].sort_values("avg_satisfaction", ascending=False).round(2)

    table = dash_table.DataTable(
        data=display_df.to_dict("records"),
        columns=[{"name": c.replace("_", " ").title(), "id": c} for c in display_cols],
        sort_action="native",
        filter_action="native",
        page_size=20,
        style_table={"overflowX": "auto"},
        style_header={"backgroundColor": "#303030", "color": "white", "fontWeight": "bold"},
        style_cell={"backgroundColor": "#1e1e1e", "color": "white", "textAlign": "center"},
    )

    return fig_sat, fig_scatter, table
