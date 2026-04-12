from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Nasłuchuję na duże transakcje (amount > 3000)...")

for message in consumer:
    data = message.value
    if data['amount'] > 3000:
        print(f"ALERT: {data['tx_id']} | {data['amount']} PLN | {data['store']} | {data['category']}")
