import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.graph_objs as go
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
import threading
from datetime import datetime

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
hdfs_csv_path_insulin = "hdfs://localhost:9000/diabete_data/insulin.csv"
hdfs_csv_path_glucose = "hdfs://localhost:9000/diabete_data/glucose.csv"
hdfs_csv_path_summary = "hdfs://localhost:9000/diabete_data/2014_10_01-10_09_39_Summary.csv"

# Shared variables to store Hadoop data and its loading status
hadoop_data_lock = threading.Lock()
hadoop_data_insulin = pd.DataFrame()
hadoop_data_glucose = pd.DataFrame()
hadoop_data_summary = pd.DataFrame()
hadoop_data_loaded = False

# Function to fetch data from Cassandra
def fetch_cassandra_data():
    query = "SELECT * FROM measurements"
    rows = session.execute(query)
    return pd.DataFrame(rows)

# Function to fetch data from Hadoop (via Spark) only once
def fetch_hadoop_data_once():
    global hadoop_data_insulin,hadoop_data_glucose,hadoop_data_summary, hadoop_data_loaded
    print("Fetching Hadoop data...")
    try:
        df_spark_insulin = spark.read.csv(hdfs_csv_path_insulin, header=True, inferSchema=True)
        df_insulin = df_spark_insulin.toPandas()

        if 'time' in df_insulin.columns:
            # Convert 'time' to datetime, using only the time part (hour, minute, second)
            df_insulin['time'] = pd.to_datetime(df_insulin['time']).dt.time
            df_insulin['datetime'] = pd.to_datetime(df_insulin['date'].astype(str) + ' ' + df_insulin['time'].astype(str))

        df_spark_glucose = spark.read.csv(hdfs_csv_path_glucose, header=True, inferSchema=True)
        df_glucose = df_spark_glucose.toPandas()

        if 'time' in df_glucose.columns:
            # Convert 'time' to datetime, using only the time part (hour, minute, second)
            df_glucose['time'] = pd.to_datetime(df_glucose['time']).dt.time
            df_glucose['datetime'] = pd.to_datetime(df_glucose['date'].astype(str) + ' ' + df_glucose['time'].astype(str))

        # df_spark_summary = spark.read.csv(hdfs_csv_path_summary, header=True, inferSchema=True)
        # df_summary = df_spark_summary.toPandas()

        # Lock the shared variable to update it safely
        with hadoop_data_lock:
            hadoop_data_insulin = df_insulin
            hadoop_data_glucose = df_glucose
            # hadoop_data_summary = df_summary
            hadoop_data_loaded = True  # Mark data as loaded
        print("Hadoop data loaded successfully.")
    except Exception as e:
        print(f"Error fetching Hadoop data: {e}")

# Start the background thread to load Hadoop data
thread = threading.Thread(target=fetch_hadoop_data_once, daemon=True)
thread.start()

# Dash app layout
app.layout = html.Div([
    html.Div(
        html.H1("Diabetes symptoms monitoring", style={'textAlign': 'center'}),
        style={'marginBottom': '20px'}  # Optional, to add space below the title
    ),
    
    # Graph for Cassandra Data (Glucose)
    html.Div([
        html.H2("Real-time Glucose level"),
        dcc.Graph(id='cassandra-glucose-graph'),  # Graph for Cassandra data
    ], style={'padding': '20px'}),
    
     html.Div([
        html.H2("Real time Lactate level vs Cortisol levels"),
        dcc.Graph(id='cassandra-lactate-cortisol-graph'),  # Lactate vs Cortisol plot
    ], style={'padding': '20px'}),

    # Graph for Cortisol vs Glucose Scatter Plot
    html.Div([
        html.H2("Real time Cortisol Level vs Glucose Level"),
        dcc.Graph(id='cassandra-cortisol-glucose-graph'),  # Cortisol vs Glucose plot
    ], style={'padding': '20px'}),

    # Graph for Hadoop Data (Insulin)
    html.Div([
        html.Div([
            html.H2("Historical Insulin Level"),
            html.Span(id='hadoop-loading-message', style={'color': 'red', 'fontWeight': 'bold', 'marginLeft': '10px'})  # Loading message next to title
        ], style={'display': 'flex', 'alignItems': 'center'}),
        dcc.Graph(id='hadoop-insulin-graph'),  # Graph for Hadoop data
    ], style={'padding': '20px'}),


    html.Div([
        html.H2("Real-Time vs Historical Glucose Level"),
        dcc.Graph(id='combined-graph'),  # Graph for both real-time and historical data
    ], style={'padding': '20px'}),

    dcc.Interval(
        id='interval-component',
        interval=1000,  # Update every 1 second
        n_intervals=0
    ),
])

@app.callback(
    [Output('cassandra-glucose-graph', 'figure'),
     Output('cassandra-lactate-cortisol-graph', 'figure'),
     Output('cassandra-cortisol-glucose-graph', 'figure'),
     Output('hadoop-insulin-graph', 'figure'),
     Output('combined-graph', 'figure'),
     Output('hadoop-loading-message', 'children')],
    [Input('interval-component', 'n_intervals')]
)
def update_graph(n):
    # Fetch Cassandra data
    cassandra_df = fetch_cassandra_data()

    if cassandra_df.empty:
        cassandra_figure = go.Figure()
        cassandra_glucose_figure = go.Figure()
    else:
        cassandra_df['timestamp'] = pd.to_datetime(cassandra_df['timestamp'])
        cassandra_df['time'] = cassandra_df['timestamp'].dt.time  # Extract time
        cassandra_df = cassandra_df.sort_values(by='timestamp')
        
        # Prepare the Cassandra glucose figure
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
                # title='Cassandra Glucose Level',
                xaxis=dict(title='Time', type='date'),
                yaxis=dict(title='Glucose Level'),
                showlegend=False,
                margin=dict(t=20, b=40, l=60, r=20)
            )
        }

        lactate_cortisol_figure = {
            'data': [
                go.Scatter(
                    x=cassandra_df['timestamp'],
                    y=cassandra_df['lactate_level'],
                    mode='lines',
                    name='Lactate Level',
                    line=dict(color='green', width=3.5)
                ),
                go.Scatter(
                    x=cassandra_df['timestamp'],
                    y=cassandra_df['cortisol_level'],
                    fill='tozeroy',
                    name='Cortisol Level',
                    line=dict(color='rgba(255, 165,0, 0.1)')
                    
                )
            ],
            'layout': go.Layout(
                # title='Lactate Level vs Cortisol Level',
                xaxis=dict(title='Time'),
                yaxis=dict(title='Level'),
                margin=dict(t=20, b=40, l=60, r=20),
                showlegend=True  # Show legend for both lines
            )
        }

        # Line Plot: Cortisol Level vs Glucose Level (with different colors for each)
        cortisol_glucose_figure = {
            'data': [
                go.Scatter(
                    x=cassandra_df['timestamp'],
                    y=cassandra_df['cortisol_level'],
                    mode='lines',
                    name='Cortisol Level',
                    line=dict(color='purple', width=3.5)
                ),
                go.Scatter(
                    x=cassandra_df['timestamp'],
                    y=cassandra_df['glucose_level'],
                    fill='tozeroy',
                    name='Glucose Level',
                    line=dict(color='rgba(255,192,203, 0.1)', width=2)
                )
            ],
            'layout': go.Layout(
                # title='Cortisol Level vs Glucose Level',
                xaxis=dict(title='Time'),
                yaxis=dict(title='Level'),
                margin=dict(t=20, b=40, l=60, r=20),
                showlegend=True  # Show legend for both lines
            )
        }
    # Fetch Hadoop data from the shared variable
    with hadoop_data_lock:
        local_hadoop_data_insulin = hadoop_data_insulin.copy()
        hadoop_glucose_data = hadoop_data_glucose.copy()

    if not hadoop_data_loaded:
        hadoop_figure = go.Figure()
        cassandra_glucose_figure = go.Figure()
        loading_message = "Loading Hadoop data..."
    elif local_hadoop_data_insulin.empty or 'datetime' not in local_hadoop_data_insulin.columns or 'fast_insulin' not in local_hadoop_data_insulin.columns:
        hadoop_figure = go.Figure()
        cassandra_glucose_figure = go.Figure()
        loading_message = "Hadoop data could not be loaded."
    else:
        local_hadoop_data_insulin['time'] = local_hadoop_data_insulin['datetime'].dt.time  # Extract time
        hadoop_figure = {
            'data': [
                go.Scatter(
                    x=local_hadoop_data_insulin['datetime'],
                    y=local_hadoop_data_insulin['fast_insulin'],
                    mode='lines',
                    name='Insulin Level',
                    line=dict(color='green', width=2)
                )
            ],
            'layout': go.Layout(
                # title='Hadoop Fast Insulin Level',
                xaxis=dict(title='Time', type='date'),
                yaxis=dict(title='Fast Insulin Level'),
                showlegend=False,
                margin=dict(t=20, b=40, l=60, r=20)
            )
        }
       
        glucose_data_converted= [value * 18.015 for value in hadoop_glucose_data['glucose']]

        hadoop_glucose_area = go.Scatter(
            x=cassandra_df['timestamp'],
            y=glucose_data_converted,  # Assuming 'glucose' column exists
            fill='tozeroy',  # Area chart below the line
            name='Historical Glucose',
            line=dict(color='rgba(0, 0, 255, 0.1)')  # Blue color with transparency
        )

        # Prepare the real-time glucose line plot from Cassandra data.
        cassandra_glucose_line = go.Scatter(
            x=cassandra_df['timestamp'],
            y=cassandra_df['glucose_level'],
            mode='lines+markers',
            name='Real-Time Glucose',
            line=dict(color='orange', width=3)
        )

        # Combine the historical (area) and real-time (line) plots into one figure.
        cassandra_glucose_figure = {
            'data': [hadoop_glucose_area, cassandra_glucose_line],
            'layout': go.Layout(
                # title='Cassandra Real-Time Glucose vs Hadoop Historical Glucose',
                xaxis=dict(title='Time', type='date'),
                yaxis=dict(title='Glucose Level'),
                showlegend=True,
                margin=dict(t=20, b=40, l=60, r=20)
            )
        }

        loading_message = ""  # Clear the loading message

    return cassandra_figure,lactate_cortisol_figure,cortisol_glucose_figure, hadoop_figure,cassandra_glucose_figure, loading_message

if __name__ == '__main__':
    app.run_server(debug=True)
