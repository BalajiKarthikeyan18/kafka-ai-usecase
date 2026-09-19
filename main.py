import os
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic

load_dotenv()

config = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVER"),
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("KAFKA_API_KEY"),
    "sasl.password": os.getenv("KAFKA_API_SECRET"),
}

admin_client = AdminClient(config)

topic_name = "mcp-test-topic"

topic = NewTopic(
    topic_name,
    num_partitions=3,
    replication_factor=3
)

print(f"Creating topic: {topic_name}")

future = admin_client.create_topics([topic])[topic_name]

try:
    future.result()
    print(f"Topic created successfully: {topic_name}")
except Exception as e:
    print(f"Failed to create topic: {e}")