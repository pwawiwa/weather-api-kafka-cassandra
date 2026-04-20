import requests
from cassandra.cluster import Cluster
from confluent_kafka.admin import AdminClient
import sys
import os
from dotenv import load_dotenv

load_dotenv()

def check_cassandra():
    print("--- Cassandra Check ---")
    try:
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect()
        session.set_keyspace('weather')
        row = session.execute("SELECT count(*) FROM weather_data").one()
        print(f"✅ Cassandra connected. Rows in weather_data: {row[0]}")
        cluster.shutdown()
        return True
    except Exception as e:
        print(f"❌ Cassandra failed: {e}")
        return False

def check_kafka():
    print("\n--- Kafka Check ---")
    try:
        conf = {'bootstrap.servers': 'localhost:9092'}
        admin = AdminClient(conf)
        metadata = admin.list_topics(timeout=10)
        topic_name = 'weather-stream'
        if topic_name in metadata.topics:
            print(f"✅ Kafka connected. Topic '{topic_name}' exists.")
        else:
            print(f"❌ Kafka connected but topic '{topic_name}' is MISSING.")
        return True
    except Exception as e:
        print(f"❌ Kafka failed: {e}")
        return False

def check_grafana():
    print("\n--- Grafana Check ---")
    try:
        response = requests.get("http://localhost:3000/api/health")
        if response.status_code == 200:
            print(f"✅ Grafana health API: {response.json()['database']}")
            
            # Check datasource
            ds_res = requests.get("http://localhost:3000/api/datasources/name/Cassandra", auth=('admin', 'admin'))
            if ds_res.status_code == 200:
                print("✅ Grafana 'Cassandra' datasource is correctly provisioned.")
            else:
                print(f"❌ Grafana datasource check failed: {ds_res.status_code}")
        else:
            print(f"❌ Grafana health API failed: {response.status_code}")
        return True
    except Exception as e:
        print(f"❌ Grafana failed: {e}")
        return False

def main():
    c = check_cassandra()
    k = check_kafka()
    g = check_grafana()
    
    print("\n" + "="*20)
    if c and k and g:
        print("ALL SYSTEMS NOMINAL!")
    else:
        print("SOME SYSTEMS FAILED. Check logs above.")
    print("="*20)

if __name__ == "__main__":
    main()
