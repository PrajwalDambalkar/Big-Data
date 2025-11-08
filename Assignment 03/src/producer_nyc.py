import time, random, json, os
import pandas as pd
from confluent_kafka import Producer

TOPIC = "nyc.trips.raw"
KAFKA = {"bootstrap.servers": "localhost:9092"}

def load_sample(path: str, n: int = 1000):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Data file not found: {path}")
    df = pd.read_parquet(path) if path.endswith(".parquet") else pd.read_csv(path)

    cols = [
        "tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count",
        "trip_distance","PULocationID","DOLocationID",
        "fare_amount","tip_amount","total_amount","payment_type"
    ]
    df = df[cols].dropna().head(n)

    # normalize for JSON serialization
    df["tpep_pickup_datetime"]  = pd.to_datetime(df["tpep_pickup_datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    return df.to_dict(orient="records")

def on_delivery(err, msg):
    if err:
        print("❌ delivery failed:", err)
    else:
        # print a few to show progress
        if msg.offset() % 50 == 0:
            print(f"✅ sent offset {msg.offset()}")

if __name__ == "__main__":
    p = Producer(KAFKA)
    data = load_sample("data/yellow_tripdata_2024-01.parquet", n=1500)
    for rec in data:
        p.produce(TOPIC, json.dumps(rec).encode("utf-8"), callback=on_delivery)
        p.poll(0)
        time.sleep(random.uniform(0.005, 0.02))  # simulate near real-time
    p.flush()
    print("✅ Producer finished.")
