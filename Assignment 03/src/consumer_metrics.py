import json, csv, os
from confluent_kafka import Consumer

TOPIC = "nyc.trips.enriched"
OUT = "nyc_metrics.csv"

c = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "nyc-sink",
    "auto.offset.reset": "earliest"
})
c.subscribe([TOPIC])

print("📥 writing processed messages to", OUT)
fieldnames = ["pickup_ts","PULocationID","payment_type","trip_distance",
              "fare_amount","tip_amount","total_amount","trip_duration_min",
              "avg_speed_mph","fare_per_mile","tip_pct","anomaly"]

exists = os.path.exists(OUT)
with open(OUT, "a", newline="") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    if not exists:
        w.writeheader()
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None: 
                continue
            if msg.error():
                print("❌", msg.error()); 
                continue
            rec = json.loads(msg.value())
            print("→", rec)
            w.writerow(rec); f.flush()
    finally:
        c.close()
