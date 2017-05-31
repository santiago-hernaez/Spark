from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    sc = SparkContext(master="local[2]", appName = "PythonStreamingTest")
    ssc = StreamingContext(sc,5)
    lines = ssc.socketTextStream("localhost", 9999)
    '''
    counts=lines.flatMap(lambda line: line.split(" "))\
            .map(lambda word: (word,1))\
            .reduceByKey(lambda a,b:a+b)
    '''
    counts=lines.flatMap((lambda line:line.split(" ")))\
            .map(lambda word:(word,1))\
            .window(15,10)\
            .transform(lambda x:x.sortByKey())
            #.reduceByKeyAndWindow(lambda x,y:x+y,0,5,10)

    counts.pprint(30)
    ssc.start()
    ssc.awaitTermination()


