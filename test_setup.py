from pyspark import SparkContext

sc= SparkContext(appName="template")

myrdd = sc.parallelize(["hola", "adios"])
print myrdd.count()
