from dash import Dash, dash_table, Input, Output, dcc, html, ctx
import dash_bootstrap_components as dbc
import pandas as pd
from sqlalchemy import create_engine
import dash
import plotly.express as px
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots

database_url = os.getenv("database_url_bbb")
mapboxtoken = os.getenv("mapboxtoken")

dash.register_page(
    __name__,
    title="Visualizations",
    path="/Visualizations",
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)

dow_dict = {
    1: "Monday",
    2: "Tuesday",
    3: "Wednesday",
    4: "Thursday",
    5: "Friday",
    6: "Saturday",
    7: "Sunday",
}

explanation_string = "This dashboard contains visualizations that shows the number of Bluebike trips over time, as well as descriptive information such as where and when people are taking trips."

n_trips_string = (
    "The number of Blue Bike trips shows an increasing trend with clear seasonality, with peaks during the summer months and valleys during the winter, "
    "which makes sense given Boston's climate. The month with the most trips is September 2022, which coincided with a shutdown of the Orange line."
    "The increasing trend is likely due to improvements in bicycling infrastructure in the Boston area, as well as an increase in the number of blue bikes and blue bike stations. "
)
member_status_string = (
    "When looking at whether trips are coming from subscribers to the program, or single use rides, "
    "we see during the winter months have the highest percent of trips completed by subscribers, ranging from 85% to 90%. "
    "Meanwhile, the summer months are when trips completed by casual users are the highest, at around 33% of all trips. We also note that September 2022 has an irregular spike in rides completed by members, suggesting the Orange line shutdown led to an increase in Blue Bike memberships. "
    "This does not appear to have been a permanent shift however, as percent of rides completed by subscribers in October seems to have returned to normal. "
)


hours_string = (
    "The graph of trips by starting hour shows that most trips begin in the late afternoon, with 5pm bneing the most popular starting hour. There's also comparatively not that many trips that begin at 8 am, suggesting blue bikes are not predominantly used by people commuting into the office."
    " When looking at day and start hour however, during week days there are clear spikes at 8am and 5pm, suggesting at least some people commute using Blue Bikes. "
    "Interestingly, these spikes go away when looking at weekend days, which have much smoother starting hour curves that peak from about 3pm to 8pm. "
    "Looking at the number of trips by day shows Saturday is the most popular day for rides, while Monday is the least popular. "
)

district_string = (
    "It's unsurprising that Boston and Cambridge have the highest percentage of trips started there, combining for almost 90% of all trips. "
    "The other cities are also more recent additions to the program, and even though they have had less trips does not necessarily imply the program is unsuccessful in those areas."
)


def serve_layout_visualizations():
    return dbc.Container(
        [
            html.H1("Boston Blue Bike Visualizations"),
            html.P(explanation_string),
            html.Hr(),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(id="n-trips-graph", figure=fig_n_trips), width=10
                    ),
                    dbc.Col(html.P(n_trips_string)),
                ]
            ),
            html.Hr(),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(
                            id="n-trips-graph-subscribers", figure=fig_n_trips_subs
                        ),
                        width=10,
                    ),
                    dbc.Col(html.P(member_status_string)),
                ]
            ),
            html.Hr(),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dbc.Row(
                                [
                                    dbc.Col(
                                        dcc.Graph(id="start-hour", figure=fig_hours),
                                        width=5,
                                    ),
                                    dbc.Col(
                                        dcc.Graph(id="days", figure=fig_days), width=5
                                    ),
                                ]
                            ),
                            dbc.Row(
                                [
                                    dbc.Col(
                                        dcc.Graph(id="day-trips", figure=fig_time_days),
                                        width=10,
                                    )
                                ]
                            ),
                        ],
                        width=10,
                    ),
                    dbc.Col(html.P(hours_string)),
                ]
            ),
            html.Hr(),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(id="district-trips", figure=fig_districts), width=10
                    ),
                    dbc.Col(html.P(district_string)),
                ]
            ),
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Graph(id="district-trips", figure=fig_boston_cambridge),
                        width=10,
                    )
                ]
            ),
        ],
        fluid=True,
    )


layout = serve_layout_visualizations

db = create_engine(database_url)
conn = db.connect()

query_get_n_trips = f"""
            SELECT month as "Date", n_trips as "Number of Trips" FROM monthly_trips
            """
dff_n_trips = pd.read_sql(query_get_n_trips, con=conn)
fig_n_trips = px.line(dff_n_trips, x="Date", y="Number of Trips")
fig_n_trips.update_layout(
    title={"text": "Number of Trips by Month", "font": {"size": 30}}
)


query_subscriber_trips = f"""
        SELECT month as "Date", member_casual as "Membership Status", s.n_trips as "Number of Trips", s.n_trips::float/mt.n_trips "Percent of Trips"
        FROM subscriber_monthly_trips s
        LEFT JOIN monthly_trips mt using (month)
        """
dff_n_trips_subs = pd.read_sql(query_subscriber_trips, con=conn)
dff_n_trips_subs = (
    dff_n_trips_subs.set_index(["Date", "Membership Status"])
    .stack(level=[0])
    .reset_index()
)
dff_n_trips_subs.columns = ["Date", "Membership Status", "Metric", "Value"]

fig_n_trips_subs = px.line(
    dff_n_trips_subs, x="Date", color="Membership Status", y="Value", facet_col="Metric"
)
fig_n_trips_subs.update_yaxes(matches=None)
fig_n_trips_subs.update_layout(
    title={
        "text": "Number of Trips and Percent of Trips by Member Status",
        "font": {"size": 30},
    }
)


query_hours = f"""
        SELECT hour as "Hour", n_trips as "Number of Trips" from hour_start_view
        """
dff_hours = pd.read_sql(query_hours, con=conn)
fig_hours = px.line(dff_hours, x="Hour", y="Number of Trips")
fig_hours.update_layout(
    title={"text": "Number of Trips Started by Hour", "font": {"size": 30}}
)


query_dow = f"""
        SELECT day as "Day", n_trips as "Number of Trips" from day_of_week_trips
        """
dff_dow = pd.read_sql(query_dow, con=conn)
dff_dow["Day"] = dff_dow["Day"].replace(dow_dict)
fig_days = px.bar(dff_dow, x="Day", y="Number of Trips")
fig_days.update_layout(
    title={"text": "Number of Trips Started by Day", "font": {"size": 30}}
)


query_hour_days = """
SELECT hour as "Hour", day as "Day", n_trips as "Number of Trips" FROM hour_day_started_at
"""
dff_hour_days = pd.read_sql(query_hour_days, con=conn)
dff_hour_days["Day"] = dff_hour_days["Day"].replace(dow_dict)
fig_time_days = px.line(
    dff_hour_days, x="Hour", y="Number of Trips", facet_col="Day", facet_col_wrap=5
)
fig_time_days.update_layout(
    title={"text": "Number of Trips started by Day, Hour", "font": {"size": 30}}
)


query_district = """
SELECT district as "District", n_trips as "Number of Trips", n_trips_percent "Percent of Trips" FROM district_counts
"""
dff_districts = pd.read_sql(query_district, con=conn)
fig_districts = px.bar(
    dff_districts,
    x="District",
    y="Number of Trips",
    hover_data=["Percent of Trips"],
    title="Number of trips started by district",
)
fig_districts.update_layout(
    title={"text": "Number of Trips Started by District", "font": {"size": 30}}
)


query_boston_cambridge = """
SELECT month as "Date", district as "District", n_trips as "Number of Trips", percent_subscriber as "Percent Subscriber" FROM boston_cambridge
"""
df_boston_cambridge = pd.read_sql(query_boston_cambridge, con=conn)
df_boston_cambridge = (
    df_boston_cambridge.set_index(["Date", "District"]).stack(level=[0]).reset_index()
)
df_boston_cambridge.columns = ["Date", "District", "Metric", "Value"]
fig_boston_cambridge = px.line(
    df_boston_cambridge,
    x="Date",
    y="Value",
    color="District",
    facet_col="Metric",
    facet_col_wrap=2,
)
fig_boston_cambridge.update_yaxes(matches=None)
fig_boston_cambridge.update_layout(
    title={
        "text": "Number of Trips and Percent Subscriber for Boston, Cambridge",
        "font": {"size": 30},
    }
)


db.dispose()
conn.close()
