import json, csv, os
from confluent_kafka import Consumer

# Configuration constants for Kafka topics and output file
TOPIC = "nyc.trips.enriched"
OUT = "nyc_metrics.csv"

# Main processing loop with file writing
if __name__ == "__main__":
    # Initialize Kafka Consumer for consuming enriched trip data
    c = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": f"nyc-sink-{__import__('time').time():.0f}",  # unique group id
        "auto.offset.reset": "earliest"
    })
    c.subscribe([TOPIC])

    # Initialize output CSV file and write header if file doesn't exist
    print("Writing processed messages to", OUT)
    fieldnames = ["pickup_ts","PULocationID","payment_type","trip_distance",
                  "fare_amount","tip_amount","total_amount","trip_duration_min",
                  "avg_speed_mph","fare_per_mile","tip_pct","anomaly"]

    exists = os.path.exists(OUT)    # Check if output file already exists
    with open(OUT, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            w.writeheader()
        try:
            # Continuously poll for new messages and write to CSV file. Then flush to ensure data is written.
            while True:
                msg = c.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    # Unknown topic/partition means the processor hasn't published yet; keep polling
                    if "UNKNOWN_TOPIC_OR_PARTITION" in str(msg.error()):
                        continue
                    print("Consumer error:", msg.error())
                    continue
                rec = json.loads(msg.value())
                print("Received:", rec)
                w.writerow(rec)
                f.flush()
        finally:
            c.close()
