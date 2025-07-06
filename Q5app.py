import pandas as pd
import plotly.express as px
from dash import Dash, html, dcc

# Load your data
df = pd.read_csv("time_of_day_patterns.csv")

# Prepare the plots
category_order = {"time_of_day": ["Night", "Morning", "Afternoon", "Evening"]}

# Chart 1: Bar chart by time and race, split by region
fig1 = px.bar(
    df, x="time_of_day", y="stop_count", color="subject_race",
    barmode="group", facet_col="region",
    category_orders=category_order,
    title="Traffic Stops by Time of Day, Race, and Region",
    labels={"stop_count": "Stop Count", "time_of_day": "Time of Day", "subject_race": "Race"}
)

# Chart 2: Stacked bar chart of stop time by race per region
fig2 = px.bar(
    df, x="region", y="stop_count", color="time_of_day",
    barmode="stack", facet_col="subject_race",
    category_orders=category_order,
    title="Distribution of Stop Times per Race, Stacked by Time of Day",
    labels={"stop_count": "Stop Count", "region": "Region", "time_of_day": "Time of Day"}
)

# Chart 3: Line plot of trends over time
line_data = df.groupby(["time_of_day", "subject_race", "region"])["stop_count"].sum().reset_index()
fig3 = px.line(
    line_data, x="time_of_day", y="stop_count", color="subject_race",
    line_group="region", facet_col="region", markers=True,
    category_orders=category_order,
    title="Stop Trends Over Time of Day by Race and Region",
    labels={"stop_count": "Stop Count", "time_of_day": "Time of Day"}
)

# Initialize Dash app
app = Dash(__name__)
app.title = "Traffic Stop Analysis"

# Layout of the app
app.layout = html.Div([
    html.H1("Q5: Interactive Traffic Stop Visualizations", style={"textAlign": "center"}),

    html.H2("1. Traffic Stops by Time of Day, Race, and Region"),
    dcc.Graph(figure=fig1),

    html.H2("2. Stop Time Distribution by Race per Region"),
    dcc.Graph(figure=fig2),

    html.H2("3. Stop Trends Over Time of Day by Race and Region"),
    dcc.Graph(figure=fig3),
])

# Run the app
if __name__ == "__main__":
    app.run(debug=True)
