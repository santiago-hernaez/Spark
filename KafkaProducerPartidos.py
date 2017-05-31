import os
import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

fileName = os.path.join('data', 'DataSetPartidos.txt')

infile = open(fileName, 'r')
for line in infile:
    producer.send("pythonmatch",line)
    time.sleep(0.5)

infile.close()
