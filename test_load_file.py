__author__ = 'bigdata'

import os.path
from pyspark import SparkContext

fileName = os.path.join('data', 'DataSetPartidos.txt')

sc = SparkContext(appName="test1")


print sc.textFile(fileName).take(5)

'''
fileRDD = (sc.textFile(fileName)
            .filter(lambda line: len(line) < 25)
            .saveAsTextFile("dirResult"))
            '''