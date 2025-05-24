import threading
import queue


class FinanceFetcher(threading.Thread):
    """
    Thread that fetches financial data using a finance client
    and pushes each batch to a shared queue.
    """
    def __init__(self, client, data_queue):
        super().__init__()
        self.client = client
        self.data_queue = data_queue

    def run(self):
        for batch in self.client.fetch_batches():
            print(f"📥 Got batch with {len(batch)} records")
            self.data_queue.put(batch)


class KafkaSender(threading.Thread):
    """
    Thread that sends data batches from the queue to Kafka using a producer.
    """
    def __init__(self, producer_manager, topic, partition_key=None, data_queue=None): 
        super().__init__()
        self.producer_manager = producer_manager
        self.topic = topic
        self.data_queue = data_queue
        self.partition_key = partition_key

    def run(self):
        while True:
            try:
                messages = self.data_queue.get(timeout=30)
                self.producer_manager.send_messages(self.topic,messages,partition_key=self.partition_key
                )
                print(f"📤 Sent batch of {len(messages)} messages")
            except queue.Empty:
                print("✅ Queue is empty, done sending.")
                break
