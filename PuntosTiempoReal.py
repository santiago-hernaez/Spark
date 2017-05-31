from __future__ import print_function
from kafka import KafkaConsumer
import sys
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
topic = "pythonmatch"
brokers = "localhost:9092"

def victoria(row): #puede hacer devolviendo equipo ganador o dos equipos y luego se asignan los puntos en otra def.
    resultado = []
    if row.golesLocal>row.golesVisitante:
        resultado.append((row.equipoLocal,3))
    elif row.golesLocal<row.golesVisitante:
        resultado.append((row.equipoVisitante,3))
    else:
        resultado.append ((row.equipoLocal,1))
        resultado.append((row.equipoVisitante,1))
    return resultado

def parseMatch(logline):

    """ Parse a line in the Apache Common Log format
    Args:
        logline (str): a line of text in the Apache Common Log format
    Returns:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0

    # A regular expression pattern to extract fields from the log line
    """
    match = logline.split("::")
    return (Row(
        idPartido=int(match[0]),
        temporada=match[1],
        jornada=int(match[2]),
        equipoLocal=match[3],
        equipoVisitante=match[4],
        golesLocal=int(match[5]),
        golesVisitante=int(match[6]),
        fecha=match[7],
        timestamp=match[8]
    ))


def updateFunc (new_values, last_sum):
        return sum(new_values) + (last_sum or 0)

print ("TEST3")

if __name__ == "__main__":
    sc = SparkContext(master="local[2]", appName = "PythonStreamingPartidos")
    ssc = StreamingContext (sc,1)
    ssc.checkpoint("checkpoint")
    partidos = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    equipo = partidos.map(lambda (x,y):y).map(parseMatch)\
                .flatMap(victoria)\
                .updateStateByKey(updateFunc) \
                .transform(lambda x: x.sortByKey(lambda (k,v):-v))

    equipo.pprint(20)
    ssc.start()
    ssc.awaitTermination()