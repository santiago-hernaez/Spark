from __future__ import print_function
from kafka import KafkaConsumer
import sys
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="PythonStreaming")
ssc = StreamingContext(sc, 10)

brokers = "localhost:9092"
topic = "zaramazonCommand"

kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

ssc.checkpoint("checkpoint")

lines = kvs.map(lambda x: x[1]) \
            .flatMap(lambda x: x.split(" ")) \
            .map(lambda w: (w,1)) \
            .reduceByKey(lambda a,b: a+b) \

lines.pprint()
ssc.start()
ssc.awaitTermination()