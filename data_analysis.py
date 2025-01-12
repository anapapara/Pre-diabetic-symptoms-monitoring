import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.graph_objs as go
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from datetime import datetime
import time

# Initialize the Dash app
app = dash.Dash(__name__)

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])
session = cluster.connect('diabet_data')

# Initialize Spark Session for Hadoop
spark = SparkSession.builder \
    .appName("CSV_to_JSON") \
    .master("local[*]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.eventLog.enabled", "true") \
    .getOrCreate()

# Path to the CSV file in HDFS
hdfs_csv_path = "hdfs://localhost:9000/diabete_data/insulin.csv"

# Function to fetch data from Cassandra
def fetch_cassandra_data():
    query = "SELECT * FROM measurements"
    rows = session.execute(query)
    return pd.DataFrame(rows)

# Function to fetch data from Hadoop (via Spark)
def fetch_hadoop_data():
    df_spark = spark.read.csv(hdfs_csv_path, header=True, inferSchema=True)

    df = df_spark.toPandas()

    if 'time' in df.columns:
        # Convert 'time' to datetime, using only the time part (hour, minute, second)
        df['time'] = pd.to_datetime(df['time']).dt.time
        df['datetime'] = pd.to_datetime(df['date'].astype(str) + ' ' + df['time'].astype(str))
      
    return df

app.layout = html.Div([
    html.H1("Real-Time Glucose and Insulin Levels"),
    
    # Graph for Cassandra Data (Glucose)
    html.Div([
        html.H3("Cassandra Glucose Level"),
        dcc.Graph(id='cassandra-glucose-graph'),  # Graph for Cassandra data
    ], style={'padding': '20px'}),
    
    # Graph for Hadoop Data (Insulin)
    html.Div([
        html.H3("Hadoop Fast Insulin Level"),
        dcc.Graph(id='hadoop-insulin-graph'),  # Graph for Hadoop data
    ], style={'padding': '20px'}),
    
    dcc.Interval(
        id='interval-component',
        interval=1000,  # Update every 1 second
        n_intervals=0
    ),
])

# Callback to update both graphs every second
@app.callback(
    [Output('cassandra-glucose-graph', 'figure'),
     Output('hadoop-insulin-graph', 'figure')],
    [Input('interval-component', 'n_intervals')]
)
def update_graph(n):
    # Fetch Cassandra data
    cassandra_df = fetch_cassandra_data()

    if cassandra_df.empty:
        cassandra_figure = go.Figure()
    else:
        cassandra_df['timestamp'] = pd.to_datetime(cassandra_df['timestamp'])
        cassandra_df = cassandra_df.sort_values(by='timestamp')
        cassandra_figure = {
            'data': [
                go.Scatter(
                    x=cassandra_df['timestamp'],
                    y=cassandra_df['glucose_level'],
                    mode='lines',
                    name='Cassandra Glucose Level',
                    line=dict(color='blue', width=2)
                )
            ],
            'layout': go.Layout(
                title='Cassandra Glucose Level',
                xaxis=dict(title='Time', type='date'),
                yaxis=dict(title='Glucose Level'),
                showlegend=False,  # No legend
                margin=dict(t=20, b=40, l=60, r=20)  # Add margins for clarity
            )
        }

    # Fetch Hadoop data
    hadoop_df = fetch_hadoop_data()

    if hadoop_df.empty or 'datetime' not in hadoop_df.columns or 'fast_insulin' not in hadoop_df.columns:
        hadoop_figure = go.Figure()
    else:
        # Plot the Hadoop data with 'datetime' on the x-axis and 'fast_insulin' on the y-axis
        hadoop_df = hadoop_df.sort_values(by='datetime')
        hadoop_figure = {
            'data': [
                go.Scatter(
                    x=hadoop_df['datetime'],
                    y=hadoop_df['fast_insulin'],
                    mode='lines',
                    name='Hadoop Insulin Level',
                    line=dict(color='green', width=2)
                )
            ],
            'layout': go.Layout(
                title='Hadoop Fast Insulin Level',
                xaxis=dict(title='Time', type='date'),
                yaxis=dict(title='Fast Insulin Level'),
                showlegend=False,  # No legend
                margin=dict(t=20, b=40, l=60, r=20)  # Add margins for clarity
            )
        }

    return cassandra_figure, hadoop_figure

if __name__ == '__main__':
    app.run_server(debug=True)