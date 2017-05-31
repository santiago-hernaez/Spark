from pyspark import SparkContext

sc = SparkContext(appName="template")

wordsList = ["cat", "elephant", 'rat', 'rat', 'cat']
wordsRDD = sc.parallelize(wordsList, 4)
unicas = wordsRDD.distinct().count()
#media = wordsRDD.map(lambda x : (x,1)).groupBy(lambda(x,y):x).map(lambda(x,y):len(y)).collect()  <-- Esta es menos optima que la siguiente
media = wordsRDD.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y).map(lambda (x,y):y).collect()
print unicas
print media