import os
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient

load_dotenv()

config = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVER"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("KAFKA_API_KEY"),
    "sasl.password": os.getenv("KAFKA_API_SECRET"),
}

admin_client = AdminClient(config)

metadata = admin_client.list_topics(timeout=10)

print("Connected to Confluent Cloud")
print(f"Cluster: {metadata.cluster_id}")
print("Existing topics:")

for topic in metadata.topics:
    print(f" - {topic}")