import time, json
from typing import Set
import requests
from confluent_kafka import Producer

# USGS Earthquake Feed URL
USGS_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
TOPIC = "eq.raw"
PRODUCER = Producer({"bootstrap.servers": "localhost:9092"})
POLL_SEC = 10 

SEEN: Set[str] = set()

def fetch_events():
    r = requests.get(USGS_URL, timeout=10)
    r.raise_for_status()
    data = r.json()
    return data.get("features", [])

# Main producer loop with deduplication and periodic polling. 
# The producer fetches earthquake events from the USGS feed every POLL_SEC seconds and produces new events to the Kafka topic.
def main():
    print("Polling USGS feed and producing to eq.raw ...")
    while True:
        try:
            features = fetch_events()
            produced = 0
            for f in features:
                eid = f.get("id")
                if not eid or eid in SEEN:
                    continue
                SEEN.add(eid)
                PRODUCER.produce(TOPIC, json.dumps(f).encode("utf-8"))
                PRODUCER.poll(0)
                produced += 1
            if produced:
                print(f"Produced {produced} new events")
        except Exception as e:
            print("Producer error:", e)
        time.sleep(POLL_SEC)  # poll feed every 10 seconds

if __name__ == "__main__":
    main()