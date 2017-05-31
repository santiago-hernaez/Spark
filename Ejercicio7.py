from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import explode,udf
from pyspark.sql.types import *

#localizacion del Hive:
warehouse_location="hdfs://localhost:9000/user/hive/warehouse"

#creamos la SparkSession anadiendo el warehouse_location para poder guardar a Hive.
#para que funcione correctamente es necesario copiar el hive-site.xml desde .../hive/conf/hive-default.xml.template
# a ../spark/conf/hive-site.xml
# ademas hay que arrancar el server de Hive: > hive --service metastore
# en este caso se ha utilizado el Hive como BBDD Derby, por ser mas comodo en local.

spark = SparkSession.builder\
    .appName ("ejercicio7")\
    .config("spark.sql.warehouse.dir",warehouse_location)\
    .enableHiveSupport()\
    .getOrCreate()

df = spark.read.json("data/pokedex.json")


#-------------------------------
# Calcular la correlaccion entre peso y altura de los pokemon
altpeso = df.select(df["height"][:4],df["weight"][:4])
floatdealtpeso = spark.createDataFrame (altpeso.rdd.map(lambda (x,y):(float(x),float(y))))
#floatdealtpeso.show()

print "La correlaccion entre peso y altura es: %f" % (floatdealtpeso.corr('_1','_2',method="pearson"))

#-------------------------------
# Anadir una columna que indique si ha evolucionado o no

df.createOrReplaceTempView("pokemon") #creamos el TempView de la tabla completa.
''''
#Hay dos formas de hacerlo, la primera con SQL y un Join: creando una nueva tabla con nombres y si esta o no
#evolucionado y luego haciendo un Join con la tabla original.

def evolucionado(x):
    if (x.prev_evolution is None):
            return Row(name=x[8],evolved="No")
    else: return Row(name=x[8],evolved="Yes")

evolveCol = spark.createDataFrame(df.rdd.map(evolucionado))
evolveCol.createOrReplaceTempView("evolve") #creamos el TempVIew de la tabla con los nombres y si han evolucionado o no
#juntamos las dos tablas con un Join
sqlPkm = spark.sql("SELECT pokemon.*, evolve.evolved from pokemon join evolve where pokemon.name = evolve.name")
'''

#La segunda forma es con withColumn.
#para utilizar una funcion dentro de withColumn es necesario importar udf de pyspark.sql.functions
#ademas de importar pyspark.sql.types para que pueda serializar la nueva columna.

def evolucion (x):
    if (x is None):
        return "No"
    else: return "Yes"

comprEvolucion= udf(evolucion,StringType())

evolucionados=df.withColumn("evolved",comprEvolucion(df.prev_evolution))
print evolucionados.show()


#-------------------------------
# Calcular las 3 Weakness mas comunes
weakness = spark.createDataFrame(df.rdd.map(lambda x: Row(weakness=x[15],cantidad=1)))
listado =  weakness.withColumn("weakness", explode("weakness"))
top3= listado.rdd.map(lambda (x,y):(y,x)).reduceByKey(lambda x,y:x+y).takeOrdered(3, lambda (x,y):-y)
print "Las 3 weaknesses mas comunes son:"
print top3

#Las 3 weaknesses mas comunes son:
#[(u'Electric', 48), (u'Ground', 45), (u'Rock', 43)]

#-------------------------------
# Registrar el DataFrame para poder usar SparkSQL. Mediante SQL seleccionar
# los pokemon que son de type=Normal y egg=Not in Eggs. Guardar ese
# resultado en MySQL SQLite Hive en una tabla PokeNormal
# ------------------------------

#Es necesario hacer un explode de la columna type porque sino el SELECT no puede comprobar el array
listadoType = df.withColumn("typeExploded", explode("type"))
listadoType.createOrReplaceTempView ("pokemonType")
#creamos la tabla en Hive.
seleccion =spark.sql("CREATE TABLE IF NOT EXISTS pokenormal as SELECT * FROM pokemonType WHERE typeExploded = 'Normal' and egg='Not in Eggs'")

#aqui muestro informacion de la BBDD y de las tablas creadas
print "Informacion de BBDD:"
print (spark.sql("show databases").show())
print (spark.catalog.listDatabases())
print "--------------------------------------------------------------"
print "Informacion de tablas:"
print (spark.sql("show tables").show())
print (spark.catalog.listTables())

#leemos la tabla del Hive
pokenorm = spark.read.table ("pokenormal")
#mostramos la tabla desde sentencia SQL
print (spark.sql("SELECT * FROM pokenormal").show())
#mostramos la tabla a partir del objeto RDD creado
print (pokenorm.show())

'''
#Eliminacion de tablas utilizadas
spark.sql("drop table pokenormal")
spark.sql("drop table pokemontype")
spark.sql("drop table pokemon")
print (spark.sql("show tables").show())
'''