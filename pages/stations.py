from dash import dash_table, Input, Output, dcc, html, ctx
import dash_bootstrap_components as dbc
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from sqlalchemy import create_engine
from datetime import date
import configparser as c
import dash
import plotly.express as px
import os

database_url = os.getenv("database_url_bbb")
mapboxtoken = os.getenv("mapboxtoken")

explanation_string_1 = "This dashboard allows users to select a station and see basic information about the station as well as visualizations of key metrics"
explanation_string_2 = "Users can select a station by scrolling through the dropdown or by clicking on any of the station in the map below"
explanation_string_3 = "Station type refers to whether riders are beginning or ending their trip at the station. "
explanation_string_4 = 'Selecting "Start" will show metrics where riders began the trip at that station, and visualizes the stations where riders ended their trip.'
explanation_string_5 = 'Selecting "End" will show metrics where riders ended their trip at that station, and visualizes the stations where riders began their trip. '

interactive_graph_string = (
    "This interactive graph allows you to visualize a variety of different metrics based on different ways to aggregate the ride date. "
    "Options include looking at rides based on quarter, month, and week, as well as by day of the week and hour of start time. Metrics include "
    "the percent of rides that were taken by members to the Bluebikes program, number of trips, median duration, median distance, and median speed"
)

flow_graph_string = (
    "This graph visualizes the hourly flow of bikes to and from the station. When a rider docks their bike at the station, the cumulative flow will increase by one, and when a rider departs from the station, the cumsum decreases by one."
    "This graph allows us to tell if a station is more popularly used as an end or as a start. The limitation of this graph is that stations often are full or empty, which means that many times the flow in or out is constrained."
)

flow_graph_string_2 = "This graph aggregates the flow of the station by each hour, allowing us to see more clearly which hours are more common to use the station as a start vs as an end"

dash.register_page(
    __name__,
    title="Station Analysis",
    path="/Stations",
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)

engine = create_engine(database_url)

max_ride_query = f"""SELECT MAX(started_at) FROM trips
                            """
max_ride_date = pd.read_sql(max_ride_query, con=engine).squeeze().date()
engine.dispose()

max_ride_date_string = max_ride_date.strftime("%Y-%m-%d")


def get_stations():
    db = create_engine(database_url)
    conn = db.connect()
    stations_query = f"""
                SELECT s.name
                FROM stations s
                LEFT JOIN trips t on s.station_id = t.start_station_id
                GROUP BY s.name
                ORDER BY COUNT(t.trip_id) desc
                """
    stations_list = pd.read_sql(stations_query, con=conn).squeeze()
    conn.close()
    db.dispose()
    return stations_list


stations = get_stations()


def serve_layout_stations():
    return dbc.Container(
        [
            html.H1("Station Analysis"),
            html.P(
                [
                    explanation_string_1,
                    html.Br(),
                    html.Br(),
                    explanation_string_2,
                    html.Br(),
                    html.Br(),
                    explanation_string_3,
                    html.Br(),
                    explanation_string_4,
                    html.Br(),
                    explanation_string_5,
                ],
                style={"fontSize": 16},
            ),
            html.Hr(),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.P("Select Station:"),
                            dbc.Col(
                                dcc.Dropdown(
                                    id="station-select-stations",
                                    value="MIT at Mass Ave / Amherst St",
                                    options=stations,
                                    clearable=False,
                                ),
                                width=9,
                            ),
                            html.P("Select Station Type:"),
                            dbc.Col(
                                dcc.Dropdown(
                                    id="station-type-select-stations",
                                    value="Start",
                                    options=["End", "Start"],
                                    clearable=False,
                                ),
                                width=4,
                            ),
                            html.P("Select Start Trip Range:"),
                            dcc.DatePickerRange(
                                id="date-range-stations",
                                min_date_allowed=date(2020, 1, 1),
                                max_date_allowed=max_ride_date,
                                initial_visible_month=max_ride_date,
                                end_date=max_ride_date,
                                start_date=date(2023, 1, 1),
                            ),
                        ],
                        width=4,
                    ),
                    dbc.Col(dcc.Graph(id="station-info-indicator"), width=7),
                ]
            ),
            html.Hr(),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="station-location-map"), width=6),
                    dbc.Col(id="table-stations", width=6),
                ]
            ),
            html.Hr(),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.P("Select Date Type:"),
                            dcc.Dropdown(
                                id="date-type-stations",
                                value="Month",
                                options=[
                                    "Quarter",
                                    "Month",
                                    "Week",
                                    "Day of Week",
                                    "Hour",
                                ],
                                clearable=False,
                            ),
                        ],
                        width=2,
                    ),
                    dbc.Col(
                        [
                            html.P("Select Metric:"),
                            dcc.Dropdown(
                                id="metric-select-stations",
                                value="Number of Trips",
                                options=[
                                    "Percent Member",
                                    "Number of Trips",
                                    "Median Duration",
                                    "Median Distance",
                                    "Median Speed",
                                ],
                                clearable=False,
                            ),
                        ],
                        width=2,
                    ),
                ]
            ),
            html.Br(style={"marginBottom": "6.5em"}),
            dbc.Row(
                [
                    dbc.Col(id="main-graph-stations", width=10),
                    dbc.Col(
                        html.P(interactive_graph_string, style={"fontSize": 16}),
                        width=2,
                    ),
                ]
            ),
            html.Hr(),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="flow-graph-stations"), width=10),
                    dbc.Col(html.P(flow_graph_string, style={"fontSize": 16})),
                ]
            ),
            html.Hr(),
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="flow-graph-stations-2"), width=10),
                    dbc.Col(html.P(flow_graph_string_2, style={"fontSize": 16})),
                ]
            ),
            dcc.Store(id="graph-data-stations"),
        ],
        fluid=True,
    )


layout = serve_layout_stations


@dash.callback(
    Output(component_id="station-location-map", component_property="figure"),
    Output(component_id="station-info-indicator", component_property="figure"),
    Output(component_id="table-stations", component_property="children"),
    Input(component_id="date-range-stations", component_property="start_date"),
    Input(component_id="date-range-stations", component_property="end_date"),
    Input(component_id="station-location-map", component_property="clickData"),
    Input(component_id="station-select-stations", component_property="value"),
    Input(component_id="station-type-select-stations", component_property="value"),
)
def plot_station(start_date, end_date, clickdata, start_station, station_type):
    station_options = {"End": "end_station_id", "Start": "start_station_id"}
    station_id_type = station_options[station_type]

    station_options_reversed = {
        "End": "start_station_id",
        "Start": "end_station_id",
    }
    reverse_station_id_type = station_options_reversed[station_type]

    db = create_engine(database_url)
    conn = db.connect()

    if station_type == "Start":
        reverse_type = "End"
    else:
        reverse_type = "Start"

    if station_type == "Start":
        preposition = "from"
    else:
        preposition = "to"

    most_recent = ctx.triggered_id
    if most_recent == "station-location-map":
        station_name = clickdata["points"][0]["text"].split("(")[0][:-1]
    else:
        station_name = start_station

    query_location = f"""
        SELECT longitude, latitude
        from stations
        where name = '{station_name}'"""

    coords = pd.read_sql(query_location, con=conn).values
    station_long, station_lat = coords[0][0], coords[0][1]

    get_end_stations_query = f"""
                SELECT s.name, s.longitude, s.latitude, COUNT(*) "Number of Trips",  
                AVG(CASE WHEN  member_casual = 'member' THEN 1 ELSE 0 END) "Percent Member",
                PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY t.duration) "Median Duration",
                PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY t.distance) "Median Distance",
                PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY (60*t.distance/t.duration)) "Median Speed"
                FROM trips t
                INNER JOIN stations s on t.{reverse_station_id_type} = s.station_id
                WHERE t.{station_id_type}  = (SELECT station_id from stations where name = '{station_name}')
                AND t.started_at between '{start_date}' and '{end_date}'
                group by s.name, s.longitude, s.latitude
                ORDER BY 4 desc
                LIMIT 25
                """

    end_stations_df = pd.read_sql(get_end_stations_query, con=conn)

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

    fig = go.Figure(layout={"height": 550})
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
            name=station_name,
            mode="markers",
            lon=[station_long, station_long],
            lat=[station_lat, station_lat],
            text=station_name,
            hoverinfo="text",
            marker=dict(symbol="marker", size=20),
        )
    )

    fig.update_layout(
        title=f"Top 25 {reverse_type} Stations {preposition} {station_name} <br><sup>From {start_date} to {end_date}</sup>",
        font={"size": 16},
        showlegend=False,
        legend={"title": {"text": "Number of Trips"}},
        mapbox=dict(
            accesstoken=mapboxtoken,
            style="dark",
            center=go.layout.mapbox.Center(lat=station_lat, lon=station_long),
            zoom=12.25,
        ),
    )
    end_stations_df = end_stations_df.drop(
        ["latitude", "longitude", "size", "name_trips"], axis=1
    ).round(2)

    table = dash_table.DataTable(
        data=end_stations_df.to_dict("records"),
        columns=[{"name": i, "id": i} for i in end_stations_df.columns],
        sort_action="native",
        page_size=12,
        # style_cell={
        #     "height": "auto",
        #     "minWidth": "100px",
        #     "width": "100px",
        #     "maxWidth": "100px",
        #     "whiteSpace": "normal",
        #     "fontSize": 10,
        # },
        # style_cell_conditional=[
        #     {
        #         "if": {"column_id": f"{reverse_type} Station"},
        #         "width": "200px",
        #         "minWidth": "200px",
        #         "maxWidth": "200px",
        #     },
        # ],
    )

    query_station_basics = f"""
    with info as (SELECT name, district,  deployment_year, total_docks
    FROM stations
    WHERE name = '{station_name}'),

    start_rides as (SELECT COUNT(start_station_id) start_rides
    from trips t
    LEFT JOIN stations s on s.station_id= t.start_station_id
    where s.name = '{station_name}' and started_at between '{start_date}' and '{end_date}'),

    end_rides as (SELECT COUNT(end_station_id) end_rides
    from trips t
    LEFT JOIN stations s on s.station_id= t.end_station_id
    where s.name = '{station_name}' and started_at between '{start_date}' and '{end_date}')

    SELECT * FROM info, start_rides, end_rides
    """

    station_info = pd.read_sql(query_station_basics, con=conn)

    indicator = go.Figure()

    indicator.add_trace(
        go.Indicator(
            value=station_info["deployment_year"].iloc[0],
            title={"text": "Deployment Year"},
            domain={"x": [0, 0.25], "y": [0, 0.5]},
        )
    )
    indicator.add_trace(
        go.Indicator(
            value=station_info["total_docks"].iloc[0],
            title={"text": "Total Docks"},
            domain={"x": [0.25, 0.5], "y": [0, 0.5]},
        )
    )
    indicator.add_trace(
        go.Indicator(
            value=station_info["start_rides"].iloc[0],
            title={"text": "Rides Started"},
            domain={"x": [0.5, 0.75], "y": [0, 0.5]},
        )
    )
    indicator.add_trace(
        go.Indicator(
            value=station_info["end_rides"].iloc[0],
            title={"text": "Rides Ended"},
            domain={"x": [0.75, 1], "y": [0, 0.5]},
        )
    )

    indicator.update_layout(
        title=f"{station_name} <br><sup>Located in {station_info['district'].iloc[0]}</sup>",
        height=300,
        font={"size": 24},
    )
    conn.close()
    db.dispose()

    return (
        fig,
        indicator,
        [
            html.Br(style={"marginBottom": "1.5em"}),
            html.H4(f"Top 25 {reverse_type} Stations {preposition} {station_name}"),
            table,
        ],
    )


@dash.callback(
    Output(component_id="station-select-stations", component_property="value"),
    Input(component_id="station-select-stations", component_property="value"),
    Input(component_id="station-location-map", component_property="clickData"),
)
def update_dropdown_value(station_start, clickdata):
    most_recent = ctx.triggered_id
    if most_recent == "station-location-map":
        station_name = clickdata["points"][0]["text"].split("(")[0][:-1]
    else:
        station_name = station_start

    return station_name


@dash.callback(
    Output(component_id="graph-data-stations", component_property="data"),
    Input(component_id="station-select-stations", component_property="value"),
    Input(component_id="date-type-stations", component_property="value"),
    Input(component_id="date-range-stations", component_property="start_date"),
    Input(component_id="date-range-stations", component_property="end_date"),
    Input(component_id="station-type-select-stations", component_property="value"),
)
def get_station_graphs_data(
    station_name, date_type, start_date, end_date, station_type
):
    station_options = {"End": "end_station_id", "Start": "start_station_id"}
    station_id_type = station_options[station_type]

    station_options_reversed = {
        "End": "start_station_id",
        "Start": "end_station_id",
    }
    reverse_station_id_type = station_options_reversed[station_type]

    db = create_engine(database_url)
    conn = db.connect()
    date_type_conversions = {
        "Quarter": "quarter",
        "Month": "month",
        "Week": "week",
        "Day of Week": "isodow",
        "Hour": "hour",
    }
    date_type_sql = date_type_conversions[date_type]
    if date_type in ["Quarter", "Month", "Week"]:
        data_query = f"""
                    SELECT date_trunc('{date_type_sql}', started_at) "Date",
                    COUNT(trip_id) "Number of Trips",
                    AVG(CASE WHEN  member_casual = 'member' THEN 1 ELSE 0 END) "Percent Member",
                    PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY t.duration) "Median Duration",
                    PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY t.distance) "Median Distance",
                    PERCENTILE_disc(0.5) WITHIN GROUP(ORDER BY (60*t.distance/t.duration)) "Median Speed"

                    FROM trips t
                    INNER JOIN stations s on t.{reverse_station_id_type} = s.station_id
                    WHERE t.{station_id_type}  = (SELECT station_id from stations where name = '{station_name}') and started_at between '{start_date}' and '{end_date}'
                    GROUP BY 1
                    ORDER BY 1
                    """
    else:
        data_query = f"""
                    SELECT extract('{date_type_sql}' from started_at) "Date",
                    COUNT(trip_id) "Number of Trips",
                    AVG(CASE WHEN  member_casual = 'member' THEN 1 ELSE 0 END) "Percent Member",
                    PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY t.duration) "Median Duration",
                    PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY t.distance) "Median Distance",
                    PERCENTILE_disc(0.5) WITHIN GROUP(ORDER BY (60*t.distance/t.duration)) "Median Speed"
                    FROM trips t
                    INNER JOIN stations s on t.{reverse_station_id_type} = s.station_id
                    WHERE t.{station_id_type}  = (SELECT station_id from stations where name = '{station_name}') and started_at between '{start_date}' and '{end_date}'
                    GROUP BY 1
                    ORDER BY 1
                        """

    data = pd.read_sql(data_query, con=conn)
    conn.close()
    db.dispose()
    return data.to_json(date_format="iso", orient="split")


@dash.callback(
    Output(component_id="main-graph-stations", component_property="children"),
    Input(component_id="graph-data-stations", component_property="data"),
    Input(component_id="metric-select-stations", component_property="value"),
    Input(component_id="station-select-stations", component_property="value"),
    Input(component_id="date-type-stations", component_property="value"),
    Input(component_id="station-type-select-stations", component_property="value"),
)
def plot_data(jsonified_data, metric, station, date_type, station_type):
    if station_type == "Start":
        preposition = "from"
    else:
        preposition = "to"
    dff = pd.read_json(jsonified_data, orient="split")
    if date_type == "Day of Week":
        dff["Date"] = dff["Date"].replace(
            {
                1: "Monday",
                2: "Tuesday",
                3: "Wednesday",
                4: "Thursday",
                5: "Friday",
                6: "Saturday",
                7: "Sunday",
            }
        )
        fig = px.line(
            dff, x="Date", y=metric, hover_data=["Number of Trips"], markers=True
        )
        fig.update_layout(
            title=f"{metric} {preposition} {station} by Day of Week",
            font={"size": 24}  # ,
            #  height=800,
        )
    else:
        fig = px.line(
            dff, x="Date", y=metric, hover_data=["Number of Trips"], markers=True
        )
        fig.update_layout(
            title=f"{date_type}ly {metric} {preposition} {station}",
            font={"size": 24}  # ,
            # height=800,
        )
    return dcc.Graph(figure=fig)


@dash.callback(
    Output(component_id="flow-graph-stations", component_property="figure"),
    Output(component_id="flow-graph-stations-2", component_property="figure"),
    Input(component_id="station-select-stations", component_property="value"),
    Input(component_id="date-range-stations", component_property="start_date"),
    Input(component_id="date-range-stations", component_property="end_date"),
)
def flow_graph(station, start_date, end_date):
    db = create_engine(database_url)
    conn = db.connect()

    find_station_id = f"""
    SELECT station_id
    FROM stations where name = '{station}'
    """
    station_id = pd.read_sql_query(find_station_id, con=conn).squeeze()

    query = f"""
    with starts as (SELECT * FROM
    (
       SELECT day
       FROM   generate_series(timestamp '{start_date}'
                            , timestamp '{end_date}'
                            , interval  '1 hour') day
       ) d
       LEFT JOIN (SELECT DATE_TRUNC('hour', started_at) as Day, COALESCE(COUNT(trip_id), 0) start_trips
    FROM trips t
    WHERE t.start_station_id = '{station_id}' and started_at between '{start_date}' and '{end_date}'
    GROUP BY 1
    ORDER BY 1) s USING(day)),

    ends as (SELECT * FROM
    (
       SELECT day
       FROM   generate_series(timestamp '{start_date}'
                            , timestamp '{end_date}'
                            , interval  '1 hour') day
       ) d
       LEFT JOIN (SELECT DATE_TRUNC('hour', started_at) as Day, COALESCE(COUNT(trip_id), 0) end_trips
    FROM trips t
    WHERE t.end_station_id = '{station_id}' and started_at between '{start_date}' and '{end_date}'
    GROUP BY 1
    ORDER BY 1) s USING(day))

    SELECT s.*,
    e.end_trips,
    COALESCE(e.end_trips, 0) - COALESCE(s.start_trips, 0)  flow,
    SUM(COALESCE(e.end_trips, 0) -COALESCE(s.start_trips, 0)) over (order by s.Day asc rows between unbounded preceding and current row) cumulative_flow
    FROM starts s LEFT JOIN ends e USING (Day)
    """

    df_flow = pd.read_sql_query(query, con=conn)
    conn.close()
    db.dispose()

    fig = px.line(df_flow, x="day", y="cumulative_flow")
    fig.update_layout(
        title=f"Hourly Flow for {station}", font={"size": 24}  # height=800,
    )

    df_flow["hour"] = df_flow["day"].dt.hour
    df_flow2 = df_flow.groupby("hour")["flow"].agg([np.mean, np.sum]).reset_index()

    fig2 = px.bar(df_flow2, x="hour", y="mean")
    fig2.update_layout(
        title=f"Average Hourly Flow for {station}", font={"size": 24}  # height=800,
    )

    return fig, fig2
