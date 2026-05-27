import dash
import dash_bootstrap_components as dbc
from dash import html, dcc

app = dash.Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[dbc.themes.DARKLY],
    suppress_callback_exceptions=True,
)

server = app.server

nav = dbc.NavbarSimple(
    children=[
        dbc.NavItem(dbc.NavLink("Overview", href="/")),
        dbc.NavItem(dbc.NavLink("Centre Comparison", href="/comparison")),
        dbc.NavItem(dbc.NavLink("Agent Leaderboard", href="/agent")),
        dbc.NavItem(dbc.NavLink("SLA Monitor", href="/sla")),
        dbc.NavItem(dbc.NavLink("Resolution Funnel", href="/resolution")),
    ],
    brand="📞 Call Center Analytics",
    brand_href="/",
    color="dark",
    dark=True,
    className="mb-3",
)

app.layout = dbc.Container(
    [nav, dash.page_container],
    fluid=True,
)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
