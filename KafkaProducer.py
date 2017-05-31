import os
import time
from kafka.client import KafkaClient
from kafka.producer import SimpleProducer


kafka = KafkaClient("localhost:9092")

producer = SimpleProducer(kafka)
fileName = os.path.join('data', 'cervantes.txt')

infile = open(fileName, 'r')
for line in infile:
    producer.send_messages("pythontest",line)
    time.sleep(0.5)

infile.close()
