from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, TopicPartition

# Configuration Kafka
conf = {
    'bootstrap.servers': 'kafka_spg:9092',
    'group.id': 'debug-consumer-group',
    'auto.offset.reset': 'earliest'
}

topic_name = 'finnhub-data'

# Étape 1 : récupérer les partitions du topic
admin = AdminClient({'bootstrap.servers': conf['bootstrap.servers']})
metadata = admin.list_topics(timeout=10)
partitions = metadata.topics[topic_name].partitions.keys()

# Étape 2 : préparer les offsets à réinitialiser
partitions_to_reset = [TopicPartition(topic_name, p, 0) for p in partitions]

# Étape 3 : assigner manuellement et repositionner les offsets
consumer = Consumer(conf)
consumer.assign(partitions_to_reset)

# Commit l'offset 0 pour chaque partition
consumer.commit(offsets=partitions_to_reset, asynchronous=False)
print(f"Offsets reset to 0 for group '{conf['group.id']}' on topic '{topic_name}'")

consumer.close()