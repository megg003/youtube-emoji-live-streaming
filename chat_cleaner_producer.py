from kafka import KafkaConsumer, KafkaProducer
import json
import re

consumer = KafkaConsumer(
    'youtube-raw-chat',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest'
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

import emoji

def clean_message(text):
    # Optional: remove URLs and convert to lowercase, but keep emojis
    text = re.sub(r'http\S+', '', text).lower()
    return text  # Keep emojis intact!

def extract_emojis(text):
    return ''.join(c for c in text if c in emoji.EMOJI_DATA)



for msg in consumer:
    raw_data = msg.value
    cleaned_message = clean_message(raw_data['message'])
    emoji_only = extract_emojis(cleaned_message)

    cleaned = {
        'user': raw_data['user'],
        'message': cleaned_message,
        'emojis': emoji_only  # ✅ Add this!
    }

    print("Cleaned & Sent:", cleaned)
    producer.send('youtube-cleaned-chat', value=cleaned)
y

