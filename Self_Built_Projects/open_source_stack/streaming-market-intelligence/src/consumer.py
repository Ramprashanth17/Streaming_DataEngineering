"""
Minimal consumer -- run this alongside the producer to visibly prove
messages are flowing, and that same-ticker events land in the same
partition (the ordering guarantee from ch.4).

Run:
    uv run src/consumer.py
"""
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "market.prices.raw",
    bootstrap_servers="localhost:9094",
    key_deserializer=lambda k: k.decode("utf-8") if k else None,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
)

print("Listening on market.prices.raw ... (Ctrl-C to stop)")
for msg in consumer:
    print(f"partition={msg.partition}  key={msg.key}  value={msg.value}")