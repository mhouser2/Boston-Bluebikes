import dash
from dash import html, Dash
import dash_bootstrap_components as dbc

app = Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.BOOTSTRAP])

server = app.server

explanation_string = (
    "Bluebikes is Boston's bike share program with more than 400 station and 4,000 bikes in the greater Boston area. "
    "This dashboard contains data on trips since 2020, aiming to understand key information about the program. "
)

sidebar = dbc.Nav(
    [
        dbc.NavLink(
            [
                html.Div(page["name"], className="ms-2"),
            ],
            href=page["path"],
            active="exact",
        )
        for page in dash.page_registry.values()
    ],
    vertical=False,
    pills=True,
    className="bg-light",
)

app.layout = dbc.Container(
    [
        dbc.Row(
            [
                dbc.Col(
                    html.Div("Boston Bluebikes Dashboards"),
                    style={"fontSize": 50, "textAlign": "center"},
                )
            ]
        ),
        dbc.Row([html.P(explanation_string)]),
        dbc.Row([dbc.Col(sidebar, width=4)]),
        html.Hr(),
        dbc.Row([dbc.Col([dash.page_container], width=12)]),
    ],
    fluid=True,
)

if __name__ == "__main__":
    server.run(debug=True)
