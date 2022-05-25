from json import loads
from kafka import KafkaConsumer


class MyConsumer():

    def __init__(self, topic: str) -> None:
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda x: loads(x.decode('utf-8'))
        )

    def consume(self):
        for message in self.consumer:
            msg = message.value
            print(f"got this message {msg}")
