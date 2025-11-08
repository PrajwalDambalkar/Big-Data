import time, random, json, os
import pandas as pd
from confluent_kafka import Producer

# Constants for Kafka topic and configuration
TOPIC = "nyc.trips.raw"
KAFKA = {"bootstrap.servers": "localhost:9092"}

# Function to load and preprocess sample data
def load_sample(path: str, n: int = 1000):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Data file not found: {path}")
    df = pd.read_parquet(path) if path.endswith(".parquet") else pd.read_csv(path) # Load data

    # Select relevant columns and drop missing values
    cols = [
        "tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count",
        "trip_distance","PULocationID","DOLocationID",
        "fare_amount","tip_amount","total_amount","payment_type"
    ]
    # Filter and preprocess the DataFrame
    df = df[cols].dropna().head(n)

    # normalize for JSON serialization
    df["tpep_pickup_datetime"]  = pd.to_datetime(df["tpep_pickup_datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    return df.to_dict(orient="records")

# Callback function for delivery reports
def on_delivery(err, msg):
    if err:
        print("Delivery failed:", err)

# Main execution block
if __name__ == "__main__":
    p = Producer(KAFKA)
    data = load_sample("data/yellow_tripdata_2024-01.parquet", n=1500) # Load sample data
    for rec in data:                # Iterate over records
        p.produce(TOPIC, json.dumps(rec).encode("utf-8"), callback=on_delivery) # Produce message
        p.poll(0)           # Trigger delivery report callbacks
        time.sleep(random.uniform(0.005, 0.02)) # Simulate near real-time
    p.flush()              # Wait for all messages to be delivered
    print("Producer finished.")     # Indicate completion
