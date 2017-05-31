
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
ssc = StreamingContext(sc, 10)

brokers = "localhost:9092"
topic = "pythontest"
kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
lines = kvs.map(lambda x: x[1]) \
            .flatMap(lambda x: x.split(" ")) \
            .map(lambda w: (w,1)) \
            .reduceByKey(lambda a,b: a+b) \

lines.pprint()
ssc.start()
ssc.awaitTermination()