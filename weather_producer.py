import requests
import json
from confluent_kafka import Producer
import time
from dotenv import load_dotenv
import os
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

conf = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
}

producer = Producer(conf)
API_KEY = os.getenv("OPENWEATHER_API_KEY")
TOPIC = os.getenv("TOPIC", "weather-stream")
INTERVAL_SECONDS = int(os.getenv("PRODUCER_INTERVAL", "10"))  # how often to push all cities

CITIES = [
    "Nairobi", "Lagos", "Accra", "Cairo", "Cape Town",
    "Addis Ababa", "Dakar", "Kampala", "Algiers",
    "Casablanca", "Kigali", "Johannesburg"
]


def get_weather(city):
    if not API_KEY or API_KEY == "your_api_key_here":
        # Mock data when no real API key is set
        return {
            'city': city,
            'timestamp': int(time.time()),
            'main': {
                'temp': round(random.uniform(18.0, 38.0), 2),
                'humidity': random.randint(30, 90)
            }
        }

    url = (
        f"https://api.openweathermap.org/data/2.5/weather"
        f"?q={city}&appid={API_KEY}&units=metric"
    )
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        data['city'] = city
        return data
    except Exception as e:
        print(f"[ERROR] Failed to fetch weather for {city}: {e}")
        return None


def delivery_report(err, msg):
    if err is not None:
        print(f"[KAFKA] Delivery failed: {err}")
    else:
        print(f"[KAFKA] {msg.key().decode() if msg.key() else '?'} → {msg.topic()}[{msg.partition()}]@{msg.offset()}")


def fetch_and_publish(city):
    weather = get_weather(city)
    if weather:
        producer.produce(
            TOPIC,
            key=city.encode('utf-8'),
            value=json.dumps(weather).encode('utf-8'),
            callback=delivery_report
        )
        return city
    return None


def main():
    print(f"Starting real-time producer — {len(CITIES)} cities every {INTERVAL_SECONDS}s → topic '{TOPIC}'")
    cycle = 0
    while True:
        cycle += 1
        start = time.time()
        print(f"\n[Cycle {cycle}] Fetching all {len(CITIES)} cities in parallel...")

        # Fetch all cities concurrently
        with ThreadPoolExecutor(max_workers=len(CITIES)) as executor:
            futures = {executor.submit(fetch_and_publish, city): city for city in CITIES}
            ok, failed = [], []
            for future in as_completed(futures):
                result = future.result()
                (ok if result else failed).append(futures[future])

        # Flush all queued messages at once
        producer.flush()

        elapsed = time.time() - start
        print(f"[Cycle {cycle}] Done — {len(ok)}/{len(CITIES)} cities sent in {elapsed:.2f}s | failed: {failed or 'none'}")

        # Sleep the remainder of the interval
        sleep_for = max(0, INTERVAL_SECONDS - elapsed)
        if sleep_for > 0:
            time.sleep(sleep_for)


if __name__ == "__main__":
    main()
