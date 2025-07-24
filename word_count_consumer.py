from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'youtube-word-counts',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest'
)

for msg in consumer:
    record = msg.value
    print(f"[{record['timestamp']}] {record['word']} → {record['count']}")

