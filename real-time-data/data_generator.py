from cassandra.cluster import Cluster
import uuid
import random
import time
from datetime import datetime
import json


# Load Ranges from JSON
with open('data_ranges.json', 'r') as file:
    ranges = json.load(file)

# Connect to Cassandra
cluster = Cluster(['127.0.0.1'])  
session = cluster.connect()

# Use the Keyspace
session.set_keyspace('diabet_data')

# Generate and Insert Data
while True:
    id = uuid.uuid4()  
    current_time = datetime.now()  # Current timestamp

    heart_rate = round(random.uniform(ranges["heart_rate"][0], ranges["heart_rate"][1]),2)
    body_temperature = round(random.uniform(ranges["body_temperature"][0], ranges["body_temperature"][1]),2)
    blood_presure = round(random.uniform(ranges["blood_presure"][0], ranges["blood_presure"][1]),2)
    glucose_level = round(random.uniform(ranges["glucose_level"][0], ranges["glucose_level"][1]),2)
    lactate_level = round(random.uniform(ranges["lactate_level"][0], ranges["lactate_level"][1]),2)
    cortisol_level = round(random.uniform(ranges["cortisol_level"][0], ranges["cortisol_level"][1]),2)
    uric_acid = round(random.uniform(ranges["uric_acid"][0], ranges["uric_acid"][1]),2)
    alcohol_level = round(random.uniform(ranges["alcohol_level"][0], ranges["alcohol_level"][1]),2)
    blood_oxygen_saturation = round(random.uniform(ranges["blood_oxygen_saturation"][0], ranges["blood_oxygen_saturation"][1]),2)
    carbon_dioxide_level = round(random.uniform(ranges["carbon_dioxide_level"][0], ranges["carbon_dioxide_level"][1]),2)

    
    # # Insert data into Cassandra
    session.execute("""
        INSERT INTO measurements (id, timestamp, heart_rate , body_temperature , blood_pressure , 
                    glucose_level , lactate_level , cortisol_level , uric_acid , alcohol_level , blood_oxygen_saturation , 
                    carbon_dioxide_level) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (id, current_time, heart_rate, body_temperature, blood_presure, glucose_level, lactate_level, cortisol_level,
          uric_acid, alcohol_level, blood_oxygen_saturation, carbon_dioxide_level))
    
    print(f"Inserted: ID={id}, Time={current_time}, HR={heart_rate}, BT={body_temperature}, AL={alcohol_level}")
    
    # Wait 4 seconds before generating the next data
    time.sleep(4)