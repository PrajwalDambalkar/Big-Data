# consumer.py
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
	"nyc_trips",
	bootstrap_servers="localhost:9092",
	auto_offset_reset="earliest",
	enable_auto_commit=True,
	group_id="consumer",
	value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Consuming NYC taxi trips...")
for msg in consumer:
	print("received:", msg.value)

# ...existing code...
# ...existing code...
