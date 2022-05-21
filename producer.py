from ensurepip import bootstrap
from kafka import KafkaProducer
from time import sleep
from json import dumps

# Creating a producer

class MyProducer():
    def __init__(self, server, topic) -> None:
        self.producer = KafkaProducer(bootstrap_servers = ['localhost:9092'],
                         value_serializer = lambda x: dumps(x).encode('utf-8'))
        self.topic = topic
        self.counter = 0
# Producer sends event with the topic numtest 
    def send(self,data):
        if self.counter == 1000:
            return
        self.producer.send(self.topic, value=data)