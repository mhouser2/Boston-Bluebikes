from dash import Dash, dash_table, Input, Output, dcc, html, ctx
import dash_bootstrap_components as dbc
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from sqlalchemy import create_engine
from datetime import date
import dash
import re
import os

database_url = os.getenv("database_url_bbb")
mapboxtoken = os.getenv("mapboxtoken")


description_string = """
This dashboard visualizes the current active Bluebike stations in Boston, colored and sized by the number of trips either started or ended there. 
Clicking on any station will update the rightmost graph and the table below. 
"""

fig_height = 650

dash.register_page(
    __name__,
    title="Station Map",
    path="/",
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)

engine = create_engine(database_url)

max_ride_query = f"""SELECT MAX(started_at) FROM trips
                            """
max_ride_date = pd.read_sql(max_ride_query, con=engine).squeeze().date()
engine.dispose()

max_ride_date_string = max_ride_date.strftime("%Y-%m-%d")


def serve_layout_station_comparison():
    return dbc.Container(
        [
            html.H1("Boston Bluebikes Station Map"),
            html.Div(description_string, style=dict(width="50%")),
            html.Hr(),
            html.H6("Select Station Type"),
            dbc.Col(
                dcc.Dropdown(
                    id="station-type",
                    value="End Station",
                    options=["Start Station", "End Station"],
                    clearable=False,
                ),
                width=3,
            ),
            html.H6("Select Start Trip Range:"),
            dcc.DatePickerRange(
                id="date-range",
                min_date_allowed=date(2020, 1, 1),
                max_date_allowed=max_ride_date,
                initial_visible_month=max_ride_date,
                end_date=max_ride_date,
                start_date=date(2023, 1, 1),
            ),
            html.Hr(),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="graph-all"), width=6),
                    dbc.Col(dcc.Graph(id="graph-specific"), width=6),
                ]
            ),
            html.Hr(),
            html.Div(id="table"),
            html.Br(),
        ],
        fluid=True,
    )


layout = serve_layout_station_comparison


@dash.callback(
    Output(component_id="graph-specific", component_property="figure"),
    Output(component_id="table", component_property="children"),
    Input(component_id="station-type", component_property="value"),
    Input(component_id="date-range", component_property="start_date"),
    Input(component_id="date-range", component_property="end_date"),
    Input(component_id="graph-all", component_property="clickData"),
    Input(component_id="graph-specific", component_property="clickData"),
)
def gather_data(station_type, start_date, end_date, clickdata, clickdata2):
    station_options = {
        "End Station": "end_station_id",
        "Start Station": "start_station_id",
    }
    station_id_type = station_options[station_type]

    station_options_reversed = {
        "End Station": "start_station_id",
        "Start Station": "end_station_id",
    }
    reverse_station_id_type = station_options_reversed[station_type]

    most_recent = ctx.triggered_id

    if most_recent == "graph-all":
        clickdata_name = re.split(r" \(\d", clickdata["points"][0]["text"])[0]
    elif most_recent == "graph-specific":
        clickdata_name = re.split(r" \(\d", clickdata2["points"][0]["text"])[0]
    else:
        clickdata_name = "MIT at Mass Ave / Amherst St"

    db = create_engine(database_url)
    conn = db.connect()
    query_location = f"""
    SELECT longitude, latitude
    from stations
    where name = '{clickdata_name}'"""

    coords = pd.read_sql(query_location, con=conn).values
    station_long, station_lat = coords[0][0], coords[0][1]

    get_end_stations_query = f"""
            SELECT s.name, s.longitude, s.latitude, COUNT(trip_id) "Number of Trips", 
            AVG(CASE WHEN  member_casual = 'member' THEN 1 ELSE 0 END) "Percent Member",
            PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY t.duration) "Median Duration",
            PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY t.distance) "Median Distance",
            PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY (60*t.distance/t.duration)) "Median Speed"
            FROM trips t
            INNER JOIN stations s on t.{reverse_station_id_type} = s.station_id
			INNER JOIN stations s2  on s2.station_id = t.{station_id_type}
            WHERE s2.name = '{clickdata_name}'
            AND t.started_at between '{start_date}' and '{end_date}'
            group by s.name, s.longitude, s.latitude
            ORDER BY 4 desc
            LIMIT 25
                """

    end_stations_df = pd.read_sql(get_end_stations_query, con=conn)
    conn.close()
    db.dispose()

    explanation_string = f"""
    The following table summarizes the end stations of trips beginning at the station located at {clickdata_name}.
    Member percent is the percent of rides done by a member to the Bluebike program, as opposed to a customer on a one time trip.
    \n\nBecause the data does not contain the exact route followed, the trip distance metric is the distance "as the crow flies."
    As such, the distance travelled by bike is always larger than found below. Relatedly, the median trip speed will also be lower than described.
    """

    if end_stations_df.empty:
        return None
    end_stations_df["size"] = (
        10
        * (
            end_stations_df["Number of Trips"]
            - end_stations_df["Number of Trips"].min()
        )
        / (
            end_stations_df["Number of Trips"].max()
            - end_stations_df["Number of Trips"].min()
        )
    ) + 10

    end_stations_df["name_trips"] = (
        end_stations_df["name"]
        + " ("
        + end_stations_df["Number of Trips"].astype(str)
        + " trips)"
    )

    fig = go.Figure(layout={"height": fig_height})
    fig.add_trace(
        go.Scattermapbox(
            mode="markers",
            lon=end_stations_df["longitude"],
            lat=end_stations_df["latitude"],
            text=end_stations_df["name_trips"],
            hoverinfo="text",
            marker=go.scattermapbox.Marker(
                colorscale="blues",
                size=end_stations_df["size"],
                allowoverlap=False,
                color=end_stations_df["Number of Trips"],
                showscale=True,
            ),
        )
    )

    fig.add_trace(
        go.Scattermapbox(
            name=clickdata_name,
            mode="markers",
            lon=[station_long, station_long],
            lat=[station_lat, station_lat],
            text=clickdata_name,
            hoverinfo="text",
            marker=dict(symbol="marker", size=20),
        )
    )

    fig.update_layout(
        title=f"Top 25 {station_type}s from {clickdata_name} <br><sup>From {start_date} to {end_date}</sup>",
        showlegend=False,
        font={"size": 16},
        mapbox=dict(
            accesstoken=mapboxtoken,
            style="dark",
            center=go.layout.mapbox.Center(lat=station_lat, lon=station_long),
            zoom=12.5,
        ),
    )

    end_stations_df = end_stations_df.drop(
        ["latitude", "longitude", "size", "name_trips"], axis=1
    ).round(2)

    table = dash_table.DataTable(
        data=end_stations_df.to_dict("records"),
        columns=[{"name": i, "id": i} for i in end_stations_df.columns],
        sort_action="native",
        style_cell={"textAlign": "left"},
        page_size=10,
    )
    return fig, dbc.Row(
        [
            dbc.Row(
                html.H4(
                    f"Top 25 {station_type} from {clickdata_name}, {start_date} to {end_date}"
                )
            ),
            dbc.Row(html.Div(explanation_string, style=dict(width="55%"))),
            dbc.Row(html.Br()),
            dbc.Row(dbc.Col(table, width=6)),
        ]
    )


@dash.callback(
    Output(component_id="graph-all", component_property="figure"),
    Input(component_id="station-type", component_property="value"),
    Input(component_id="date-range", component_property="start_date"),
    Input(component_id="date-range", component_property="end_date"),
)
def main_graph(station_type, start_date, end_date):
    station_options = {
        "End Station": "end_station_id",
        "Start Station": "start_station_id",
    }
    station_id_type = station_options[station_type]

    db = create_engine(database_url)
    conn = db.connect()

    if start_date == "2023-01-01" and end_date == max_ride_date_string:
        if station_id_type == "end_station_id":
            query = """
                    SELECT * FROM station_map_end_id
                    """
        else:
            query = """
                    SELECT * FROM station_map_start_id
                    """
    else:
        query = f"""
        select s.name, s.latitude, s.longitude, n_trips 
            from
            stations s
            INNER join
            (
            select {station_id_type}, count({station_id_type}) as n_trips
            from
            trips
            where started_at between '{start_date}' and '{end_date}'
            group by {station_id_type}
            ) trip_count_subquery
            on s.station_id=trip_count_subquery.{station_id_type}
        """
    data = pd.read_sql(query, con=conn)
    data["n_trips"] = data["n_trips"].fillna(0)
    conn.close()
    db.dispose()

    data["size"] = np.log(data["n_trips"])
    data["name_trips"] = data["name"] + " (" + data["n_trips"].astype(str) + " trips)"

    start_long = data[data["n_trips"] == data["n_trips"].max()]["longitude"].values[0]
    start_lat = data[data["n_trips"] == data["n_trips"].max()]["latitude"].values[0]

    fig = go.Figure(
        go.Scattermapbox(
            text=data["name_trips"],
            lat=data["latitude"],
            lon=data["longitude"],
            mode="markers",
            hoverinfo="text",
            marker=go.scattermapbox.Marker(
                colorscale="blues",
                size=data["size"],
                color=data["n_trips"],
                showscale=True,
            ),
        ),
        layout={"height": fig_height},
    )

    fig.update_layout(
        title=f"Top {station_type}s in Boston <br><sup>From {start_date} to {end_date}</sup>",
        font={"size": 16},
        mapbox=dict(
            accesstoken=mapboxtoken,
            style="dark",
            center=go.layout.mapbox.Center(lat=start_lat, lon=start_long),
            zoom=11,
        ),
    )
    return fig
