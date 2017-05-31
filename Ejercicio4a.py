import re
import datetime
import os

from pyspark.sql import Row
from pyspark import SparkContext

month_map = {'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
    'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12}

APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)" (\d{3}) (\S+)'

def parse_apache_time(s):

    """ Convert Apache time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format
    Returns:
        datetime: datetime object (ignore timezone for now)
    """
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))


def parseApacheLogLine(logline):

    """ Parse a line in the Apache Common Log format
    Args:
        logline (str): a line of text in the Apache Common Log format
    Returns:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0

    # A regular expression pattern to extract fields from the log line
    """
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = long(0)
    else:
        size = long(match.group(9))
    return (Row(
        host          = match.group(1),
        client_identd = match.group(2),
        user_id       = match.group(3),
        date_time     = parse_apache_time(match.group(4)),
        method        = match.group(5),
        endpoint      = match.group(6),
        protocol      = match.group(7),
        response_code = int(match.group(8)),
        content_size  = size
    ), 1)

def parseLogs():

    """ Read and parse log file """
    logFile = os.path.join('data', 'apache.access.log_small')
    #logFile = os.path.join('data', 'apache.access.log.PROJECT')
    sc = SparkContext(appName="template")

    parsed_logs = (sc
                   .textFile(logFile)
                   .map(parseApacheLogLine)
                   .cache())

    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache())

    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]))
    failed_logs_count = failed_logs.count()
    if failed_logs_count > 0:
        print 'Number of invalid logline: %d' % failed_logs.count()
        for line in failed_logs.take(20):
            print 'Invalid logline: %s' % line

    print 'Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs.count(), access_logs.count(), failed_logs.count())
    return parsed_logs, access_logs, failed_logs

parsed_logs, access_logs, failed_logs = parseLogs()

#Minimo maximo y media del tamano de las peticiones
content_size = access_logs.map(lambda x: x.content_size).cache()
minimo = content_size.min()
maximo =  minimo = content_size.max()
media = content_size.mean()
#media = content_size.reduce(lambda a,b:a+b)/content_size.count() <-- otra forma de hacer la media
print "Minimo:%i, Maximo: %s, Media %i" % (minimo, maximo, media)

#N de peticiones por cada codigo de respuesta
respuesta = access_logs.map(lambda x: (x.response_code,1)).reduceByKey(lambda x,y: x+y)
respuestalista=respuesta.take(100) #<--- guardo en el objeto de Python respuestalista los 100 respuestas (maximo)
print "peticiones por codigo de respuesta: %r y hay %r respuestas diferentes" % (respuestalista,len(respuestalista))

#Mostrar 20 hosts que hayan sido visitado mas de 10 veces
hosts= access_logs.map(lambda x: (x.host,1)).reduceByKey(lambda x,y:x+y).filter(lambda (x,y): y>10).takeOrdered(20, lambda (x,y): -1*y)
#otra opcion es hacer en el ultimo paso un map(lambda x: x[0]).take(20) porque no dice los 20 mas visitados...
print hosts

#Mostrar los 10 endpoints mas visitados
endpoints = access_logs.map(lambda x: (x.endpoint,1)).reduceByKey(lambda x,y:x+y).cache()
print endpoints.takeOrdered(10,lambda (x,y):-1*y)

#Mostrar los 10 endpoints mas visitados que no tienen codigo de respuesta 200
endpoints2 = access_logs.map(lambda x: (x.endpoint,x.response_code))\
            .filter(lambda (x,y):y<>200)\
            .map(lambda x:(x,1))\
            .reduceByKey(lambda x,y:x+y).cache()
print endpoints2.takeOrdered(10,lambda (x,y):-1*y)

#Calcular el n de Hosts distintos
hostDistint = access_logs.map(lambda x: x.host).distinct()
print hostDistint.count()

#Contar el n de Hosts unicos cada dia
hostUnico = access_logs.map(lambda x: (x.date_time.day,x.host)).distinct().countByKey()
print hostUnico
#otra forma es con .map(lambda(x,y):(x,len(set(y))) <-- el len(set(y) me muestra los valores unicos de y

#Calculas la media de peticiones diarias por Host
mediaHost = access_logs.map(lambda x:(x.date_time.day,x.host)).groupByKey()\
    .sortByKey(lambda x:x)\
    .map(lambda (x,y):(x,len(y)/len(set(y)))).collect() #<--- OJO a la forma de calculo de la media
print "Media de peticiones por host:"
for i in mediaHost:
    print str(i[0])+":"+str(i[1])

#Mostrar una lista de 40 endpoints distintos que muestran codigo de repuesta 404
badRecords=access_logs.filter(lambda log:log.response_code==404).cache()

badEndpoints=badRecords.map(lambda log:(log.endpoint,1)).distinct().take(40)
print badEndpoints
#[(u'/history/apollo/a-001/a-001-patch-small.gif', 1), (u'/shuttle/resources/orbiters/discovery.gif', 1), (u'/history/apollo/apollo-13.html', 1), (u'/www/software/winvn/winvn.html', 1), (u'/pub/winvn/release.txt', 1), (u'/pub/winvn/readme.txt', 1), (u'/history/history.htm', 1), (u'/history/apollo/a-001/movies/', 1), (u'/history/apollo/a-004/a-004-patch-small.gif', 1), (u'/sts-71/launch/', 1), (u'/history/apollo/a-004/movies/', 1), (u'/history/apollo/a-001/images/', 1), (u'/elv/DELTA/uncons.htm', 1)]


#Mostrar el Top 25 de Endpoints que mas 404 generan
topEndpoints = badRecords.map(lambda log:(log.endpoint,1)).reduceByKey(lambda a,b: a+b).takeOrdered(25,lambda (x,y):-y)
print topEndpoints
#[(u'/history/apollo/a-001/a-001-patch-small.gif', 4), (u'/pub/winvn/release.txt', 4), (u'/history/apollo/a-001/movies/', 2), (u'/pub/winvn/readme.txt', 2), (u'/history/apollo/a-004/a-004-patch-small.gif', 2), (u'/sts-71/launch/', 1), (u'/history/apollo/a-004/movies/', 1), (u'/elv/DELTA/uncons.htm', 1), (u'/history/apollo/a-001/images/', 1), (u'/history/apollo/apollo-13.html', 1), (u'/shuttle/resources/orbiters/discovery.gif', 1), (u'/www/software/winvn/winvn.html', 1), (u'/history/history.htm', 1)]


#el top 5 de dias que se generaron 404
topDias = badRecords.map(lambda dia:(dia.date_time.day,1)).reduceByKey(lambda x,y:x+y).takeOrdered(5,lambda (x,y):-y)
print topDias
#[(1,22)]
