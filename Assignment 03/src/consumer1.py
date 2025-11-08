from confluent_kafka import Consumer

TOPIC = "test.simple"

c = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "demo-consumer",
    "auto.offset.reset": "earliest"
})
c.subscribe([TOPIC])

print("👂 Listening on", TOPIC)
try:
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("❌ Consumer error:", msg.error())
            continue
        print(f"📥 Received: {msg.value().decode()} (partition {msg.partition()}, offset {msg.offset()})")
finally:
    c.close()
