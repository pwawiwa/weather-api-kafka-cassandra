import json
from cassandra.cluster import Cluster
from confluent_kafka import Consumer
import os
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "weather-stream")

def main():
    # Cassandra connection
    print(f"Connecting to Cassandra at {CASSANDRA_HOST}...")
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()
    session.set_keyspace('weather')

    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'weather-group-live', 
        'auto.offset.reset': 'latest'
    }

    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])

    print(f"Listening for weather data on topic '{TOPIC}'...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[KAFKA ERROR] {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                
                # Get temperature and humidity
                temp = data.get('main', {}).get('temp', data.get('temperature'))
                humidity = data.get('main', {}).get('humidity', data.get('humidity'))
                
                # Get timestamp: use 'dt' from OpenWeather or 'timestamp' from mock
                # Fallback to now() if missing
                raw_ts = data.get('dt', data.get('timestamp'))
                if raw_ts:
                    # Explicitly use UTC to avoid local timezone shifts
                    ts = datetime.fromtimestamp(raw_ts, tz=timezone.utc)
                else:
                    ts = datetime.now(timezone.utc)

                session.execute(
                    """
                    INSERT INTO weather_data (city, timestamp, temperature, humidity)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (data['city'], ts, temp, humidity)
                )
                print(f"Stored data for {data['city']} at {ts}")
                    
            except Exception as e:
                print(f"[INSERT ERROR] Failed for {msg.value()[:50]}: {e}")
                
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        cluster.shutdown()

if __name__ == "__main__":
    main()
