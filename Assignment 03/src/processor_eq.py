import json, math, datetime as dt
from confluent_kafka import Consumer, Producer

IN_TOPIC = "eq.raw"
OUT_TOPIC = "eq.enriched"

import datetime as dt
def iso_utc(ms_since_epoch: int) -> str:
    return dt.datetime.fromtimestamp(ms_since_epoch / 1000.0, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def region_from_place(place: str) -> str:
    if not place:
        return "Unknown"
    # Typical formats: "10 km NW of Town, State" or "Region Name"
    parts = place.split(" of ")
    tail = parts[-1] if len(parts) > 1 else place
    return tail.strip()

def severity_from_mag(m: float) -> str:
    if m is None: return "unknown"
    if m < 2.0: return "micro"
    if m < 4.0: return "minor"
    if m < 5.0: return "light"
    if m < 6.0: return "moderate"
    if m < 7.0: return "strong"
    if m < 8.0: return "major"
    return "great"

def felt_intensity(m: float, depth_km: float) -> float:
    # simple attenuation: higher depth reduces felt intensity
    if m is None or depth_km is None: return 0.0
    return round(m * math.exp(-max(depth_km, 0.0)/100.0), 3)

if __name__ == "__main__":
    c = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "eq-processor",
        "auto.offset.reset": "earliest"
    })
    p = Producer({"bootstrap.servers": "localhost:9092"})
    c.subscribe([IN_TOPIC])

    print("Processing earthquakes into eq.enriched ...")
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None: 
                continue
            if msg.error():
                print("Consumer error:", msg.error())
                continue

            try:
                f = json.loads(msg.value())
                props = f.get("properties", {}) or {}
                geom  = f.get("geometry", {}) or {}
                coords = geom.get("coordinates", [None, None, None])

                mag = props.get("mag", None)
                if mag is None:
                    continue  # skip malformed

                depth_km = coords[2] if len(coords) > 2 else None
                place = props.get("place", "")
                ts_ms = props.get("time", None)
                if ts_ms is None:
                    continue

                out = {
                    "id": f.get("id"),
                    "event_time": iso_utc(ts_ms),
                    "region": region_from_place(place),
                    "mag": float(mag),
                    "depth_km": float(depth_km) if depth_km is not None else None,
                    "severity": severity_from_mag(float(mag)),
                    "felt_intensity": felt_intensity(float(mag), float(depth_km) if depth_km is not None else 0.0),
                    "place": place
                }
                p.produce(OUT_TOPIC, json.dumps(out).encode("utf-8"))
                p.poll(0)

            except Exception as e:
                print("Skipped feature:", e)
    finally:
        c.close()
        p.flush()