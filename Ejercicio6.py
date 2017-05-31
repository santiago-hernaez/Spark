import os.path
from pyspark.sql import Row
from pyspark import SparkContext, SQLContext

fileName = os.path.join('data', 'DataSetPartidos.txt')
sc = SparkContext(appName="PythonSQL")

sqlContext = SQLContext(sc)

# Load a text file and convert each line to a Row.
lines = sc.textFile(fileName)
parts = lines.map(lambda l: l.split("::"))
partidos = parts.map(lambda p: Row(idpartido=p[0], temporada=p[1], jornada=p[2], equipoLocal=p[3], \
                                 equipoVisitante=p[4], golesLocal=int(p[5]), golesVisitante=int(p[6]), \
                                fecha=p[7], timestamp=p[8]))
print partidos.take(1)

# Infer the schema, and register the DataFrame as a table.
DFpartido = sqlContext.createDataFrame(partidos)
DFpartido.registerTempTable("partidos")
'''
ganadores= sqlContext.sql("SELECT temporada, equipoLocal, golesLocal FROM partidos WHERE golesLocal > 5")

for i in ganadores.collect():
    print i.temporada+ " " + i.equipoLocal + " " + str(i.golesLocal)
'''
partidosSporting = partidos.map(lambda x: (x[2],x[0])).groupBy(lambda x: x[1]).map(lambda x:(x[0],sum(x[1])))
DFpartidoSp= sqlContext.createDataFrame(partidosSporting)
DFpartidoSp.registerTempTable("partidosSp")
print partidosSporting.collect()
masPrimera = sqlContext.sql("SELECT _1, count(_2) FROM partidosSp")
print masPrimera


