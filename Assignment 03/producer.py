import json, time, random
import pandas as pd
from kafka import KafkaProducer

TOPIC = "nyc_trips"
producer = KafkaProducer(
	bootstrap_servers="localhost:9092",
	value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def load_sample(path, n=1000):
	df = pd.read_parquet(path) if path.endswith(".parquet") else pd.read_csv(path)
	cols = ['tpep_pickup_datetime','tpep_dropoff_datetime','passenger_count',
			'trip_distance','PULocationID','DOLocationID',
			'fare_amount','tip_amount','total_amount','payment_type']
	df = df[cols].dropna().head(n)
	return df.to_dict(orient="records")

if __name__ == "__main__":
	data = load_sample("data/yellow_tripdata_2024-01.parquet", n=500)
	for rec in data:
		producer.send(TOPIC, rec)
		print("sent:", rec)
		time.sleep(random.uniform(0.01, 0.05))
	producer.flush()
	print("Completed sending taxi data.")
