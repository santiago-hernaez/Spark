from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def updateFunc (new_values, last_sum):
        return sum(new_values) + (last_sum or 0)

if __name__ == "__main__":

    sc = SparkContext(master="local[2]", appName = "PythonStreamingTest")
    ssc = StreamingContext(sc,5)
    ssc.checkpoint("checkpoint")
    lines = ssc.socketTextStream("localhost", 9999)

    counts=lines.flatMap((lambda line:line.split(" ")))\
            .map(lambda word:(word,1))\
            .updateStateByKey(updateFunc)\
            .transform(lambda x:x.sortByKey())
            # el updateStateByKey nos sirve para aculumar en el RDD los valores de cuantas veces ha salido cada palabra
            # y ahora el stream acumula el numero de veces que sale cada palabra.


    counts.pprint(30)
    ssc.start()
    ssc.awaitTermination()



