from faker import Faker
from google.cloud import pubsub_v1
import json

fake = Faker()

project_id = "gdab-430616"
topic_id = "gdab"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

def generate_data():
    return {
        'id': fake.uuid4(),
        'name': fake.first_name(),
        'email': fake.email(),
        'timestamp': fake.iso8601()
    }

def publish_messages():
    for _ in range(1000):  # Gera e publica 1000 mensagens
        data = generate_data()
        message = json.dumps(data).encode('utf-8')
        publisher.publish(topic_path, data=message)
        print(f"Publicado: {data}")

if __name__ == "__main__":
    publish_messages()
