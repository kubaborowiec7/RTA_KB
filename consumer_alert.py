import json
from confluent_kafka import Consumer, KafkaError
from datetime import datetime, timedelta

# Konfiguracja konsumenta
conf = {
    'bootstrap.servers': 'broker:9092', # Zmień na swój adres brokera
    'group.id': 'anomaly-detector-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['transactions']) # Zmień na nazwę swojego topicu

# Słownik do przechowywania historii transakcji użytkowników: {user_id: [lista_timestampow]}
user_history = {}

print("Nasłuchiwanie transakcji w poszukiwaniu anomalii...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Błąd konsumenta: {msg.error()}")
            continue

        # Parsowanie danych
        data = json.loads(msg.value().decode('utf-8'))
        user_id = data.get('user_id')
        # Zakładamy, że w danych jest pole 'timestamp', jeśli nie - używamy czasu systemowego
        current_time = datetime.now() 

        if user_id not in user_history:
            user_history[user_id] = []

        # Dodaj aktualną transakcję do historii
        user_history[user_id].append(current_time)

        # Usuń transakcje starsze niż 60 sekund
        one_minute_ago = current_time - timedelta(seconds=60)
        user_history[user_id] = [t for t in user_history[user_id] if t > one_minute_ago]

        # Logika alertu: więcej niż 3 transakcje w ciągu 60 sekund
        if len(user_history[user_id]) > 3:
            print(f"!!! ALERT ANOMALII !!!")
            print(f"Użytkownik: {user_id}")
            print(f"Liczba transakcji w ostatniej minucie: {len(user_history[user_id])}")
            print(f"Czas transakcji: {[t.strftime('%H:%M:%S') for t in user_history[user_id]]}")
            print("-" * 30)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
