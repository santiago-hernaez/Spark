import os.path
from pyspark import SparkContext


fileName = os.path.join('data','cervantes.txt')

sc = SparkContext(appName="template")

#print sc.textFile(fileName).take(5)
fileRDD = sc.textFile(fileName)

''' fileRDD = (sc.textFile(fileName).filter (lambda line:len(line)<25).saveAsTextFile("dirResult")) '''
def wordcount (x):
    return x.map(lambda x:(x,1)).reduceByKey(lambda k,v:k+v).takeOrdered(10, lambda (x,y): -1*y)

def clean(x):
    return x.replace('.',' ').replace (',',' ').replace (";"," ").replace("  "," ")

def stopWords(word):
    badWords = ["el","la","los","las","un","uno","una","de","que","y","en","a","no","se","con","por","le","su","lo","del","como","me","es","mas","mas","al","si","yo","el","ni","sus"]
    return word not in badWords

palabras = fileRDD.flatMap(lambda x: x.split(" "))
cleanCervantesRDD = palabras.map(clean).filter(stopWords)
dict = wordcount(cleanCervantesRDD)
for i in dict:
        print i[0]+":"+str(i[1])



