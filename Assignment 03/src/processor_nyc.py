import json, datetime as dt
from confluent_kafka import Consumer, Producer

IN_TOPIC  = "nyc.trips.raw"
OUT_TOPIC = "nyc.trips.enriched"
KAFKA = {"bootstrap.servers": "localhost:9092"}

# Helper function to parse datetime strings
def to_dt(s: str) -> dt.datetime:
    try:
        return dt.datetime.fromisoformat(s)
    except Exception:
        return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")

# Main processing loop
if __name__ == "__main__":
    # Initialize Kafka Consumer and Producer which will be used in processing
    c = Consumer({
        "bootstrap.servers": KAFKA["bootstrap.servers"],
        "group.id": "nyc-processor",
        "auto.offset.reset": "earliest"
    })
    p = Producer(KAFKA)         # Initialize Producer
    c.subscribe([IN_TOPIC])     # Subscribe to input topic

    # Main processing loop
    print("Processing NYC taxi trips...")
    try:
        # Continuously poll for new messages
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error:", msg.error())
                continue
            
            # Parse the incoming message
            r = json.loads(msg.value())
            try:
                pu = to_dt(r["tpep_pickup_datetime"])
                do = to_dt(r["tpep_dropoff_datetime"])
                dur_min = (do - pu).total_seconds() / 60.0
                if dur_min <= 0:
                    continue
                
                # Extract relevant fields and compute additional metrics. Then apply quality checks. 
                dist = float(r["trip_distance"])
                fare = float(r["fare_amount"])
                tip = float(r.get("tip_amount", 0.0))
                total = float(r.get("total_amount", fare + tip))
                speed = dist / (dur_min / 60.0) if dur_min > 0 else 0.0
                tip_pct = (tip / fare) if fare > 0 else 0.0
                fare_per_mile = (fare / dist) if dist > 0 else None

                # quality guardrails
                if not (0 < dist < 100 and 0 < dur_min < 240):
                    continue
                
                # Prepare enriched record for output topic with anomaly detection
                enriched = {
                    "pickup_ts": pu.strftime("%Y-%m-%d %H:%M:%S"),
                    "PULocationID": int(r["PULocationID"]),
                    "payment_type": int(r["payment_type"]),
                    "trip_distance": round(dist, 3),
                    "fare_amount": round(fare, 2),
                    "tip_amount": round(tip, 2),
                    "total_amount": round(total, 2),
                    "trip_duration_min": round(dur_min, 2),
                    "avg_speed_mph": round(speed, 2),
                    "fare_per_mile": round(fare_per_mile, 2) if fare_per_mile else None,
                    "tip_pct": round(tip_pct, 3),
                    "anomaly": (speed > 80) or (fare < 0) or (total < 0)
                }
                # Produce enriched record to output topic
                p.produce(OUT_TOPIC, json.dumps(enriched).encode("utf-8"))
                p.poll(0)

                if msg.offset() % 100 == 0:
                    print("Processed sample:", enriched)
            except Exception as e:
                print("Skipped record:", e)
    finally:
        c.close()
        p.flush()
