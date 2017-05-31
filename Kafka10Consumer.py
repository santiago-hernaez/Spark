from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers='localhost:9092',auto_offset_reset='earliest')
consumer.subscribe(['pythontest'])
for message in consumer:
    print (message)