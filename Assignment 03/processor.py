import json, datetime as dt
from kafka import KafkaConsumer, KafkaProducer

# Initialize Kafka Consumer and Producer
consumer = KafkaConsumer(
	"nyc_trips",
	bootstrap_servers="localhost:9092",
	auto_offset_reset="earliest",
	enable_auto_commit=True,
	group_id="processor",
	value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

producer = KafkaProducer(
	bootstrap_servers="localhost:9092",
	value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("Processing NYC taxi trips...")

# Function to convert string to datetime
def to_dt(s):
	try:
		return dt.datetime.fromisoformat(s)
	except Exception:
		return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")

# Process messages from consumer
for msg in consumer:
	rec = msg.value

	# Extract and compute required fields
	try:
		pu = to_dt(rec["tpep_pickup_datetime"])
		do = to_dt(rec["tpep_dropoff_datetime"])
		dur_min = (do - pu).total_seconds() / 60.0
		if dur_min <= 0: continue
		dist = float(rec["trip_distance"])
		speed = dist / (dur_min / 60.0)
		tip_pct = float(rec["tip_amount"]) / float(rec["fare_amount"]) if rec["fare_amount"] > 0 else 0

		# Prepare output record
		out = {
			"PULocationID": rec["PULocationID"],
			"trip_distance": dist,
			"trip_duration_min": round(dur_min, 2),
			"avg_speed_mph": round(speed, 2),
			"fare_amount": rec["fare_amount"],
			"tip_pct": round(tip_pct, 3)
		}

		# Send processed record to new topic
		producer.send("nyc_trips_processed", out)
		print("Processed:", out)
	except Exception as e:
		print("skip:", e)
