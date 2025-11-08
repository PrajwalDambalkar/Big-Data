import json, csv, os
from confluent_kafka import Consumer

TOPIC = "eq.metrics"    # set to "eq.enriched" if you want raw enriched rows instead
OUT = "eq_metrics.csv"

def main():
    c = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "eq-sink",
        "auto.offset.reset": "earliest"
    })
    c.subscribe([TOPIC])

    print(f"Writing {TOPIC} messages to {OUT}")
    exists = os.path.exists(OUT)
    with open(OUT, "a", newline="") as f:
        w = None
        try:
            while True:
                msg = c.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print("Consumer error:", msg.error()); 
                    continue
                rec = json.loads(msg.value())
                print("Received:", rec)

                if w is None:
                    # initialize CSV header from first record's keys
                    w = csv.DictWriter(f, fieldnames=list(rec.keys()))
                    if not exists:
                        w.writeheader()

                w.writerow(rec); f.flush()
        finally:
            c.close()

if __name__ == "__main__":
    main()