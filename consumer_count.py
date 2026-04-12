from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = {}
msg_count = 0

for message in consumer:
    data = message.value
    store = data['store']
    amount = data['amount']
    
    store_counts[store] += 1
    
    if store not in total_amount:
        total_amount[store] = 0.0
    total_amount[store] += amount
    
    msg_count += 1
    
    if msg_count % 10 == 0:
        print(f"{'Sklep':<12} | {'Liczba':<6} | {'Suma':<10} | {'Średnia':<10}")
        for s, count in store_counts.items():
            suma = total_amount[s]
            srednia = suma / count
            print(f"{s:<12} | {count:<6} | {suma:<10.2f} | {srednia:<10.2f}")
        print("-" * 47)
