from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='stats_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

category_stats = defaultdict(lambda: {'count': 0, 'sum': 0.0, 'min': float('inf'), 'max': 0.0})
msg_count = 0

for message in consumer:
    data = message.value
    category = data['category']
    amount = data['amount']
    
    stats = category_stats[category]
    stats['count'] += 1
    stats['sum'] += amount
    
    if amount < stats['min']:
        stats['min'] = amount
    if amount > stats['max']:
        stats['max'] = amount
        
    msg_count += 1
    
    if msg_count % 10 == 0:
        print(f"{'Kategoria':<15} | {'Liczba':<6} | {'Suma':<10} | {'Min':<8} | {'Max':<8}")
        for cat, s in category_stats.items():
            print(f"{cat:<15} | {s['count']:<6} | {s['sum']:<10.2f} | {s['min']:<8.2f} | {s['max']:<8.2f}")
        print("-" * 59)
