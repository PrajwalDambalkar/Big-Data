import json, time, datetime as dt
from collections import defaultdict, Counter
from confluent_kafka import Consumer, Producer

IN_TOPIC  = "eq.enriched"
OUT_TOPIC = "eq.metrics"
WINDOW_SEC = 60  # 1 minute

def window_start(ts_iso: str) -> int:
    t = dt.datetime.strptime(ts_iso, "%Y-%m-%dT%H:%M:%SZ")
    return int(t.timestamp() // WINDOW_SEC) * WINDOW_SEC

if __name__ == "__main__":
    c = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "eq-aggregator",
        "auto.offset.reset": "earliest"
    })
    p = Producer({"bootstrap.servers": "localhost:9092"})
    c.subscribe([IN_TOPIC])

    # state[region][winstart] = accumulator
    state = defaultdict(lambda: defaultdict(lambda: {
        "count": 0,
        "sum_mag": 0.0,
        "max_mag": 0.0,
        "severity_counts": Counter()
    }))
    prev_counts = {}  # previous window count per region

    print("Aggregating eq.enriched into 5-minute metrics ...")
    try:
        while True:
            msg = c.poll(0.5)
            if msg is None:
                # periodic flush of completed windows
                now = int(time.time())
                to_flush = []
                for region, wins in list(state.items()):
                    for ws, acc in list(wins.items()):
                        if now - ws >= WINDOW_SEC:
                            cnt = acc["count"]
                            metrics = {
                                "window_start": dt.datetime.utcfromtimestamp(ws).strftime("%Y-%m-%dT%H:%M:%SZ"),
                                "window_end": dt.datetime.utcfromtimestamp(ws + WINDOW_SEC).strftime("%Y-%m-%dT%H:%M:%SZ"),
                                "region": region,
                                "count": cnt,
                                "avg_mag": round(acc["sum_mag"]/cnt, 3) if cnt else 0.0,
                                "max_mag": round(acc["max_mag"], 3),
                                "severity_breakdown": dict(acc["severity_counts"]),
                                "alert": None
                            }
                            prev = prev_counts.get(region)
                            if any(s in acc["severity_counts"] for s in ("strong","major","great")):
                                metrics["alert"] = "strong_or_higher"
                            elif prev and cnt > prev * 2 and cnt >= 3:
                                metrics["alert"] = "count_spike"
                            prev_counts[region] = cnt
                            p.produce(OUT_TOPIC, json.dumps(metrics).encode("utf-8"))
                            p.poll(0)
                            to_flush.append((region, ws))
                for region, ws in to_flush:
                    del state[region][ws]
                    if not state[region]:
                        del state[region]
                continue

            if msg.error():
                print("Consumer error:", msg.error()); 
                continue

            try:
                rec = json.loads(msg.value())
                ws = window_start(rec["event_time"])
                region = rec["region"]
                mag = float(rec["mag"])
                sev = rec["severity"]
                acc = state[region][ws]
                acc["count"] += 1
                acc["sum_mag"] += mag
                acc["max_mag"] = max(acc["max_mag"], mag)
                acc["severity_counts"][sev] += 1
            except Exception as e:
                print("Skipped enriched record:", e)

    finally:
        c.close()
        p.flush()