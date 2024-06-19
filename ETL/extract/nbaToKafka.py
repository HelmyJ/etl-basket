import os
import requests
from kafka import KafkaProducer
import json
import time

# Configuration
API_URL = "https://www.balldontlie.io/api/v1/stats"
KAFKA_TOPIC = "nba_data"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
API_KEY = os.getenv('BALLDONTLIE_API_KEY')

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_data_from_api():
    response = requests.get(API_URL, headers={"Authorization": f"Bearer {API_KEY}"})
    if response.status_code == 200:
        data = response.json()
        return data['data']  # Adjust based on the structure of the API response
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return []

def push_data_to_kafka(data):
    for record in data:
        producer.send(KAFKA_TOPIC, record)
        print(f"Sent record to Kafka: {record}")

if __name__ == "__main__":
    while True:
        data = fetch_data_from_api()
        if data:
            push_data_to_kafka(data)
        time.sleep(86400)  # Wait for one day (86400 seconds) before fetching data again