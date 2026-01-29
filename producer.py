from kafka import KafkaProducer
from faker import Faker
import json
import time
from datetime import datetime, timedelta
import random

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'user_activity'
pages = ['/home', '/products', '/cart', '/checkout']

def generate_event():
    now = datetime.utcnow().isoformat() + 'Z'
    # Occasionally late data (>2min old)
    if random.random() < 0.1:
        now = (datetime.utcnow() - timedelta(minutes=3)).isoformat() + 'Z'
    event_type = random.choice(['page_view', 'session_start', 'session_end', 'click'])
    if event_type == 'session_end':
        user_id = random.choice(['user1', 'user2'])  # Match prior starts
    else:
        user_id = fake.uuid4()
    return {
        "event_time": now,
        "user_id": user_id,
        "page_url": random.choice(pages),
        "event_type": event_type
    }

if __name__ == '__main__':
    for _ in range(100):
        event = generate_event()
        producer.send(topic, value=event)
        print(f"Sent: {event}")
        time.sleep(1)
    producer.flush()
