import os
import dash
import duckdb
import pandas as pd
import plotly.express as px
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Input, Output

dash.register_page(__name__, path="/", name="Overview", order=0)

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/data/call_center.duckdb")

CARD_STYLE = {"textAlign": "center", "padding": "1rem"}
NO_DATA = html.Div("Pipeline has not run yet — trigger the Airflow DAG first.",
                   className="text-warning mt-4 text-center")


def query(sql: str) -> pd.DataFrame:
    try:
        with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
            return con.execute(sql).df()
    except Exception:
        return pd.DataFrame()


layout = dbc.Container([
    html.H2("Executive Overview", className="mt-3 mb-4"),
    dbc.Row(id="kpi-row", className="mb-4 g-3"),
    dbc.Row([
        dbc.Col(dcc.Graph(id="calls-by-centre"), md=6),
        dbc.Col(dcc.Graph(id="daily-volume"), md=6),
    ], className="mb-4"),
    dbc.Row([
        dbc.Col(dcc.Graph(id="topic-pie"), md=6),
        dbc.Col(dcc.Graph(id="satisfaction-dist"), md=6),
    ]),
    dcc.Interval(id="refresh", interval=60_000, n_intervals=0),
], fluid=True)


@callback(
    Output("kpi-row", "children"),
    Output("calls-by-centre", "figure"),
    Output("daily-volume", "figure"),
    Output("topic-pie", "figure"),
    Output("satisfaction-dist", "figure"),
    Input("refresh", "n_intervals"),
)
def update(_):
    df_agent = query("SELECT * FROM marts.mart_agent_performance")
    df_topic = query("SELECT * FROM marts.mart_topic_analysis")
    df_sla = query("SELECT * FROM marts.mart_sla_compliance")

    if df_agent.empty:
        empty_fig = px.bar(title="No data yet")
        return [NO_DATA], empty_fig, empty_fig, empty_fig, empty_fig

    total_calls = int(df_agent["total_calls"].sum())
    avg_answer = round(df_agent["answer_rate_pct"].mean(), 1) if "answer_rate_pct" in df_agent else 0
    avg_sat = round(df_agent["avg_satisfaction"].mean(), 2) if "avg_satisfaction" in df_agent else 0
    avg_speed = round(df_agent["avg_speed_of_answer_secs"].mean(), 1) if "avg_speed_of_answer_secs" in df_agent else 0

    kpi_cards = [
        dbc.Col(dbc.Card([
            dbc.CardBody([html.H2(f"{total_calls:,}", className="card-title text-info"),
                          html.P("Total Calls", className="card-text")])
        ]), md=3),
        dbc.Col(dbc.Card([
            dbc.CardBody([html.H2(f"{avg_answer}%", className="card-title text-success"),
                          html.P("Avg Answer Rate", className="card-text")])
        ]), md=3),
        dbc.Col(dbc.Card([
            dbc.CardBody([html.H2(f"{avg_sat}/5", className="card-title text-warning"),
                          html.P("Avg Satisfaction", className="card-text")])
        ]), md=3),
        dbc.Col(dbc.Card([
            dbc.CardBody([html.H2(f"{avg_speed}s", className="card-title text-danger"),
                          html.P("Avg Speed of Answer", className="card-text")])
        ]), md=3),
    ]

    centre_df = df_agent.groupby("source_centre", as_index=False)["total_calls"].sum()
    fig_centre = px.bar(
        centre_df, x="source_centre", y="total_calls", color="source_centre",
        title="Total Calls by Centre", labels={"source_centre": "Centre", "total_calls": "Calls"},
        template="plotly_dark",
    )

    if not df_sla.empty and "call_date" in df_sla.columns:
        daily = df_sla.groupby(["call_date", "source_centre"], as_index=False)["total_calls"].sum()
        fig_daily = px.line(
            daily.sort_values("call_date"), x="call_date", y="total_calls",
            color="source_centre", title="Daily Call Volume by Centre",
            labels={"call_date": "Date", "total_calls": "Calls", "source_centre": "Centre"},
            template="plotly_dark",
        )
    else:
        fig_daily = px.bar(title="Daily volume unavailable", template="plotly_dark")

    topic_df = df_topic.groupby("topic", as_index=False)["total_calls"].sum()
    fig_topic = px.pie(
        topic_df, names="topic", values="total_calls",
        title="Call Volume by Topic", template="plotly_dark",
    )

    sat_df = query("SELECT source_centre, satisfaction_band, call_count FROM marts.mart_satisfaction_cohorts")
    if not sat_df.empty:
        sat_agg = sat_df.groupby(["satisfaction_band", "source_centre"], as_index=False)["call_count"].sum()
        order = ["Promoter (5)", "Satisfied (4)", "Neutral (3)", "Dissatisfied (2)", "Detractor (1)", "Not Rated"]
        fig_sat = px.bar(
            sat_agg, x="satisfaction_band", y="call_count", color="source_centre",
            barmode="group", category_orders={"satisfaction_band": order},
            title="Satisfaction Band Distribution",
            labels={"satisfaction_band": "Band", "call_count": "Calls", "source_centre": "Centre"},
            template="plotly_dark",
        )
    else:
        fig_sat = px.bar(title="Satisfaction data unavailable", template="plotly_dark")

    return kpi_cards, fig_centre, fig_daily, fig_topic, fig_sat
