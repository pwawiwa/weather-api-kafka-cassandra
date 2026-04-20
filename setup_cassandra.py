from cassandra.cluster import Cluster
import time
import os
from dotenv import load_dotenv

load_dotenv()

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")

def setup_db():
    print(f"Connecting to Cassandra at {CASSANDRA_HOST}...")
    
    # Retry logic for Cassandra startup
    retries = 10
    cluster = None
    while retries > 0:
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect()
            break
        except Exception as e:
            print(f"Waiting for Cassandra... ({retries} retries left)")
            time.sleep(5)
            retries -= 1
    
    if not cluster:
        print("Could not connect to Cassandra.")
        return

    print("Creating Keyspace...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS weather
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)

    print("Creating Table...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS weather.weather_data (
            city TEXT,
            timestamp TIMESTAMP,
            temperature FLOAT,
            humidity INT,
            PRIMARY KEY (city, timestamp)
        );
    """)
    
    print("Cassandra setup complete!")
    cluster.shutdown()

if __name__ == "__main__":
    setup_db()
