from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='anomaly_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_transactions = defaultdict(list)

print("Nasłuchuję na anomalie (więcej niż 3 transakcje / 60s)...")

for message in consumer:
    data = message.value
    user_id = data['user_id']
    current_time = datetime.fromisoformat(data['timestamp'])
    
    user_transactions[user_id].append(current_time)
    
    recent_transactions = [t for t in user_transactions[user_id] if (current_time - t).total_seconds() <= 60]
    
    user_transactions[user_id] = recent_transactions
    
    if len(recent_transactions) > 3:
        print(f"ALERT! Użytkownik {user_id} wykonał {len(recent_transactions)} transakcji w ciągu ostatnich 60 sekund!")
        
