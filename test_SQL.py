__author__ = 'bigdata'

import os
import sys

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row, StructField, StructType, StringType, IntegerType

# idPartido::temporada::jornada::EquipoLocal::EquipoVisitante::golesLocal::golesVisitante::fecha::timestamp



sc = SparkContext(appName="PythonSQL")

sqlContext = SQLContext(sc)
fileName = os.path.join('data', 'DataSetPartidos.txt')

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

ganadores= sqlContext.sql("SELECT temporada, equipoLocal, golesLocal FROM partidos WHERE golesLocal > 5")

for i in ganadores.collect():
    print i.temporada+ " " + i.equipoLocal + " " + str(i.golesLocal)



