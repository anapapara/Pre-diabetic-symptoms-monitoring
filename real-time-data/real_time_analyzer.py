import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.graph_objs as go
from cassandra.cluster import Cluster
from datetime import datetime
import time

# Initialize the Dash app
app = dash.Dash(__name__)

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('diabet_data')

# Fetch data from Cassandra
def fetch_data():
    query = "SELECT * FROM measurements"
    rows = session.execute(query)
    return pd.DataFrame(rows)

app.layout = html.Div([
    html.H1("Real-Time Glucose Levels"),
    dcc.Graph(id='glucose-graph'),  
    dcc.Interval(
        id='interval-component',
        interval=1000,  # Update every 1 second
        n_intervals=0
    ),
])

# Callback to update the graph every second
@app.callback(
    Output('glucose-graph', 'figure'),
    [Input('interval-component', 'n_intervals')]
)
def update_graph(n):
    df = fetch_data()

    if df.empty:
        return go.Figure()

    df['timestamp'] = pd.to_datetime(df['timestamp'])

    df = df.sort_values(by='timestamp')

    time_hist = df['timestamp']
    glucose_hist = df['glucose_level']

    # Convert the timestamp to a datetime object if it's not already
    if isinstance(time_hist.iloc[-1], str): 
        time_hist = pd.to_datetime(time_hist)

    figure = {
        'data': [
            go.Scatter(
                x=time_hist,
                y=glucose_hist,
                mode='lines', 
                name='Glucose Level',
                line=dict(color='blue', width=2)
            )
        ],
        'layout': go.Layout(
            title='Real-Time Glucose Level',
            xaxis=dict(title='Time', type='date'),
            yaxis=dict(title='Glucose Level'),
            showlegend=True,
        )
    }

    return figure

if __name__ == '__main__':
    app.run_server(debug=True)