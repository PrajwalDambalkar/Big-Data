from confluent_kafka import Producer
import time

TOPIC = "test.simple"
p = Producer({"bootstrap.servers": "localhost:9092"})

def dr(err, msg):
    if err:
        print("❌ Delivery failed:", err)
    else:
        print(f"✅ Sent to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}: {msg.value().decode()}")

if __name__ == "__main__":
    for i in range(1, 21):
        payload = f"hello-{i}"
        p.produce(TOPIC, payload.encode("utf-8"), callback=dr)
        p.poll(0)          # serve delivery callbacks
        time.sleep(0.05)   # slow down just to see it
    p.flush()
    print("✅ Producer done.")
