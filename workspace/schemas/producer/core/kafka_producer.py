import json
from kafka import KafkaProducer


class ProducerManager:
    """
    Wrapper around KafkaProducer to send JSON-encoded messages to a topic.
    """
    def __init__(self, broker):
        self.producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            api_version=(2, 8)
        )

    def send_messages(self, topic, messages, partition_key=None):
        print(f"🛠 Sending to topic: {topic}, number of messages: {len(messages)}")

        for msg in messages:
            try:
                key = msg.get(partition_key) if partition_key else None
                self.producer.send(topic, key=key, value=msg)
            except Exception as e:
                print(f"❌ Failed to send message: {e}")
                raise

        self.producer.flush()

