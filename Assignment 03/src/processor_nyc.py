import json, datetime as dt
from confluent_kafka import Consumer, Producer

IN_TOPIC  = "nyc.trips.raw"
OUT_TOPIC = "nyc.trips.enriched"
KAFKA = {"bootstrap.servers": "localhost:9092"}

c = Consumer({
    "bootstrap.servers": KAFKA["bootstrap.servers"],
    "group.id": "nyc-processor",
    "auto.offset.reset": "earliest"
})
p = Producer(KAFKA)
c.subscribe([IN_TOPIC])

def to_dt(s):
    try:
        return dt.datetime.fromisoformat(s)
    except Exception:
        return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")

print("🚕 processing NYC taxi trips...")
try:
    while True:
        msg = c.poll(1.0)
        if msg is None: 
            continue
        if msg.error():
            print("❌ consumer error:", msg.error())
            continue

        r = json.loads(msg.value())
        try:
            pu = to_dt(r["tpep_pickup_datetime"])
            do = to_dt(r["tpep_dropoff_datetime"])
            dur_min = (do - pu).total_seconds()/60.0
            if dur_min <= 0:  # drop bad durations
                continue

            dist = float(r["trip_distance"])
            fare = float(r["fare_amount"])
            tip  = float(r.get("tip_amount", 0.0))
            total = float(r.get("total_amount", fare + tip))

            speed = dist / (dur_min/60.0) if dur_min > 0 else 0.0
            tip_pct = (tip / fare) if fare > 0 else 0.0
            fare_per_mile = (fare / dist) if dist > 0 else None

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

            p.produce(OUT_TOPIC, json.dumps(enriched).encode("utf-8"))
            p.poll(0)
            # print a sample every ~50 messages
            if msg.offset() % 50 == 0:
                print("✅ processed sample:", enriched)
        except Exception as e:
            print("↪️ skip row:", e)
finally:
    c.close()
    p.flush()
