from pyspark import SparkContext

sc= SparkContext(appName="template")

wordsList = ["cat", "elephant",'rat','rat','cat']
wordsRDD = sc.parallelize(wordsList,4)
def plural (x):
        return x+'s'
plu= wordsRDD.map(plural)
contaplu = plu.map(lambda y : len(y))
pares = plu.map (lambda x : (x,1)).cache() #lo cacheo porque lo vamos a utilizar mas de una vez...
cuentaReduce = pares.reduceByKey(lambda a,b:a+b).collect() #a,b no son clave valor, son valor 1 valor 2 valor 3...
cuentaGroup = pares.groupByKey().map(lambda (k,v):(k,sum(v))).collect() #podria ser map(lambda x:(x[0],sum(x[1])))
print cuentaGroup
print cuentaReduce
