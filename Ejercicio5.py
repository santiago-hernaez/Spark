import os.path
from pyspark.sql import Row
from pyspark import SparkContext

logFile = os.path.join('data', 'DataSetPartidos.txt')

sc = SparkContext(appName="template")


def parseApacheLogLine(logline):

    """ Parse a line in the Apache Common Log format
    Args:
        logline (str): a line of text in the Apache Common Log format
    Returns:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0

    # A regular expression pattern to extract fields from the log line
    """
    match = logline.split("::")
    #if match is None:
    #    return (logline, 0)

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

def parseLogs():

    """ Read and parse log file """

    parsed_logs = (sc
                   .textFile(logFile)
                   .map(parseApacheLogLine)
                   .cache())

    print 'Read all lines, successfully parsed %d lines' % (parsed_logs.count())
    return parsed_logs

partidos = parseLogs()
acc = sc.accumulator(0)
SPORTING = "Sporting de Gijon"

#goles marcados por el Sporting de Gijon

def gijonFilter(row):
    if ((row.golesLocal==0) and (row.golesVisitante==0)):
        acc.add(1)
    return row.equipoLocal==SPORTING or row.equipoVisitante==SPORTING


def golesGijon(row):
    if row.equipoLocal == SPORTING:
        return row.equipoLocal,row.golesLocal
    elif row.equipoVisitante == SPORTING:
        return row.equipoVisitante, row.golesVisitante

total = partidos.filter(gijonFilter).map(golesGijon).reduceByKey(lambda a,b:a+b).map(lambda (x,y):y).collect()
print "Goles marcados por el Sporting de Gijon: %d" % total[0]
print "Numero de partidos 0-0:" + str(acc.value)

# Goles marcados por el Sporting de Gijon: 2042
# Numero de partidos 0-0:3511



#En que temporada se marcaron mas goles
def temporadaGoles(row):
    return row.temporada, row.golesVisitante+row.golesLocal

#aqui guardo los goles por cada temporada, que utilizare mas adelante.
temp = partidos.map(temporadaGoles).reduceByKey(lambda a,b:a+b).cache()

temporada = temp.max(lambda (k,v):v)
print "temporada: %s con %i goles" % (str(temporada[0]), int(temporada[1]))

#temporada: 2012-13 con 2294 goles


#Equipo con record de goles como local
#Equipo con record de goles como visitante

def recordLocal(row):
    return row.equipoLocal,row.golesLocal
def recordVisitante(row):
    return row.equipoVisitante,row.golesVisitante
recordLocal = partidos.map(recordLocal).reduceByKey(lambda a,b:a+b).max(lambda (k,v):v)
recordVisitante = partidos.map(recordVisitante).reduceByKey(lambda a,b:a+b).max(lambda (k,v):v)
print "Equipo con mas goles como local: %s con %i goles" % (str(recordLocal[0]),int(recordLocal[1]))
print "Equipo con mas goles como visitante: %s con %i goles" % (str(recordVisitante[0]),int(recordVisitante[1]))

#Equipo con mas goles como local: Real Madrid con 2054 goles
#Equipo con mas goles como visitante: Real Madrid con 1296 goles

#Las 3 decadas en las que mas goles se metieron

decadas = temp.sortByKey().map(lambda (x,y):(x[2],y)).reduceByKey(lambda a,b:a+b).takeOrdered(3,lambda (x,y):-y)
options = { 7 : "Setenta",
            8 : "Ochenta",
            9 : "Noventa",
            0 : "DosMil",
            1 : "Dos mil diez"}
print "Las tres decadas con mas goles son : %s:%i goles, %s:%i goles y  %s:%i goles" % (options[int(decadas[0][0])],int(decadas[0][1]),options[int(decadas[1][0])],int(decadas[1][1]),options[int(decadas[2][0])],int(decadas[2][1]))

#Las tres decadas con mas goles son : DosMil:20759 goles, Noventa:19426 goles y  Ochenta:17351 goles


# Mejor local de los ultimos 5 anos

def victoriaLocal(row):
    if row.golesLocal>row.golesVisitante:
        return row.equipoLocal,3
    elif row.golesLocal==row.golesVisitante:
        return row.equipoLocal,1
    else:
        return row.equipoLocal,0

mejorLocal = partidos.filter(lambda x: x.temporada[0:3]>"2009").map(victoriaLocal).reduceByKey(lambda a,b:a+b).max(lambda (k,v):v)
print "El mejor Local de los ultimos 5 anos es el: %s con %i puntos ganados" % (str(mejorLocal[0]),int(mejorLocal[1]))
#El mejor Local de los ultimos 5 anos es el: Barcelona con 82 partidos ganados


#Media de victorias por temporada de los equipos que han estado menos de 10 anos en 1 division

equiposMenores = partidos.map(lambda x: (x.equipoLocal,x.temporada[0:4])).groupByKey().map(lambda (x,y):(x,set(y))).filter(lambda (x,y):len(y)<10).map(lambda (x,y):x).collect()
#print (list((j[0],list (j[1])) for j in equiposMenores))
#una vez tengo los equipos que han estado menos de 10 anos en 1 division, hay que hallar la media de victorias

def comprueba (row,equipo):
    if row.equipoLocal == equipo:
        return True
    elif row.equipoVisitante == equipo:
        return True
    else:
        return False

def victoria(row,equipo):
    if row.equipoLocal==equipo:
            if row.golesLocal>row.golesVisitante:
                return row.temporada,1
            else:
                return row.temporada,0
    else:
            if row.golesVisitante>row.golesLocal:
                return row.temporada,1
            else:
                return row.temporada,0


dictMedias = {}
for i in range(len(equiposMenores)):
    equipo = equiposMenores[i]
    print equipo
    mediaVictorias = partidos.filter(lambda x: comprueba(x,equipo)).\
    map(lambda x: victoria(x,equipo)).\
        groupByKey().\
        map(lambda (x,y):(x,len(y)/len(set(y)))).\
        sortByKey().collect()
    dictMedias[equipo]=mediaVictorias
# filtramos los equiposMenores
# mapeamos las victorias de cada partido de cada temporada (temporada,0-1)
# agrupamos por temporada
# Hallamos la media de victorias por temporada
# ordenamos y sacamos a un diccionario, con Clave: equipo y una lista con la temporada y Media de victorias/temporada


for equipo in dictMedias:
    print "El %s tuvo las siguientes medias:" % equipo
    print dictMedias[equipo]
    print "-------------------------------------------------------"
'''
El Pontevedra tuvo las siguientes medias:
[(u'1970-71', 19), (u'1971-72', 19), (u'1972-73', 19), (u'1976-77', 19), (u'2004-05', 21)]
El Cartagena tuvo las siguientes medias:
[(u'1982-83', 19), (u'1983-84', 19), (u'1984-85', 19), (u'1985-86', 19), (u'1986-87', 22), (u'1987-88', 19)]
El Ensidesa tuvo las siguientes medias:
[(u'1975-76', 19)]
El Orihuela tuvo las siguientes medias:
[(u'1990-91', 19)]
El FC Cartagena tuvo las siguientes medias:
[(u'2009-10', 21), (u'2010-11', 21), (u'2011-12', 21)]
El Guadalajara tuvo las siguientes medias:
[(u'2011-12', 21), (u'2012-13', 21)]
El Sestao tuvo las siguientes medias:
[(u'1985-86', 19), (u'1986-87', 22), (u'1987-88', 19), (u'1988-89', 19), (u'1989-90', 19), (u'1990-91', 19), (u'1991-92', 19), (u'1992-93', 19), (u'1995-96', 19)]
El Aviles tuvo las siguientes medias:
[(u'1990-91', 19), (u'1991-92', 19)]
El Cultural Leonesa tuvo las siguientes medias:
[(u'1971-72', 19), (u'1972-73', 19), (u'1974-75', 19)]
El Universidad Las Palmas tuvo las siguientes medias:
[(u'2000-01', 21)]
El Toledo tuvo las siguientes medias:
[(u'1993-94', 19), (u'1994-95', 19), (u'1995-96', 19), (u'1996-97', 19), (u'1997-98', 21), (u'1998-99', 21), (u'1999-00', 21)]
El Villarreal B tuvo las siguientes medias:
[(u'2009-10', 21), (u'2010-11', 21), (u'2011-12', 21)]
El Extremadura tuvo las siguientes medias:
[(u'1994-95', 19), (u'1995-96', 19), (u'1996-97', 21), (u'1997-98', 21), (u'1998-99', 19), (u'1999-00', 21), (u'2000-01', 21), (u'2001-02', 21)]
El Algeciras tuvo las siguientes medias:
[(u'1978-79', 19), (u'1979-80', 19), (u'1983-84', 19), (u'2003-04', 21)]
El Ponferradina tuvo las siguientes medias:
[(u'2006-07', 21), (u'2010-11', 21), (u'2012-13', 21), (u'2013-14', 21), (u'2014-2015', 17)]
El Figueres tuvo las siguientes medias:
[(u'1986-87', 22), (u'1987-88', 19), (u'1988-89', 19), (u'1989-90', 19), (u'1990-91', 19), (u'1991-92', 19), (u'1992-93', 19)]
El Ecija tuvo las siguientes medias:
[(u'1995-96', 19), (u'1996-97', 19)]
El Alzira tuvo las siguientes medias:
[(u'1988-89', 19)]
El Mestalla tuvo las siguientes medias:
[(u'1971-72', 19), (u'1972-73', 19)]
El Sant Andreu tuvo las siguientes medias:
[(u'1970-71', 19), (u'1971-72', 19), (u'1972-73', 19), (u'1973-74', 19), (u'1974-75', 19), (u'1975-76', 19), (u'1976-77', 19)]
El Barakaldo tuvo las siguientes medias:
[(u'1972-73', 19), (u'1973-74', 19), (u'1974-75', 19), (u'1977-78', 19), (u'1978-79', 19), (u'1980-81', 19)]
El Girona tuvo las siguientes medias:
[(u'2008-09', 21), (u'2009-10', 21), (u'2010-11', 21), (u'2011-12', 21), (u'2012-13', 21), (u'2013-14', 21), (u'2014-2015', 17)]
El Alcoyano tuvo las siguientes medias:
[(u'2011-12', 21)]
El Ejido tuvo las siguientes medias:
[(u'2001-02', 21), (u'2002-03', 21), (u'2003-04', 21), (u'2004-05', 21), (u'2005-06', 21), (u'2006-07', 21), (u'2007-08', 21)]
El Getafe Deportivo tuvo las siguientes medias:
[(u'1976-77', 19), (u'1977-78', 19), (u'1978-79', 19), (u'1979-80', 19), (u'1980-81', 19), (u'1981-82', 19)]
El Huesca tuvo las siguientes medias:
[(u'2008-09', 21), (u'2009-10', 21), (u'2010-11', 21), (u'2011-12', 21), (u'2012-13', 21)]
El Langreo tuvo las siguientes medias:
[(u'1970-71', 19), (u'1971-72', 19)]
El Ourense tuvo las siguientes medias:
[(u'1973-74', 19), (u'1974-75', 19), (u'1994-95', 19), (u'1996-97', 19), (u'1997-98', 21), (u'1998-99', 21)]
El Llagostera tuvo las siguientes medias:
[(u'2014-2015', 17)]
El Real Union tuvo las siguientes medias:
[(u'2009-10', 21)]
El Mollerussa tuvo las siguientes medias:
[(u'1988-89', 19)]
El Granada 74 tuvo las siguientes medias:
[(u'2007-08', 21)]
El Racing Ferrol tuvo las siguientes medias:
[(u'1970-71', 19), (u'1971-72', 19), (u'1978-79', 19), (u'2000-01', 21), (u'2001-02', 21), (u'2002-03', 21), (u'2004-05', 21), (u'2005-06', 21), (u'2007-08', 21)]
El Jaen tuvo las siguientes medias:
[(u'1976-77', 19), (u'1977-78', 19), (u'1978-79', 19), (u'1997-98', 21), (u'2000-01', 21), (u'2001-02', 21), (u'2013-14', 21)]
El Ciudad de Murcia tuvo las siguientes medias:
[(u'2003-04', 21), (u'2004-05', 21), (u'2005-06', 21), (u'2006-07', 21)]
El Ceuta tuvo las siguientes medias:
[(u'1980-81', 19)]
El Lorca Deportiva tuvo las siguientes medias:
[(u'2005-06', 21), (u'2006-07', 21)]
El Alicante tuvo las siguientes medias:
[(u'2008-09', 21)]
El Moscardo tuvo las siguientes medias:
[(u'1970-71', 19)]
El Terrassa tuvo las siguientes medias:
[(u'1975-76', 19), (u'1976-77', 19), (u'1977-78', 19), (u'1978-79', 19), (u'2002-03', 21), (u'2003-04', 21), (u'2004-05', 21)]
El Merida tuvo las siguientes medias:
[(u'1991-92', 19), (u'1992-93', 19), (u'1993-94', 19), (u'1994-95', 19), (u'1995-96', 21), (u'1996-97', 19), (u'1997-98', 19), (u'1998-99', 21), (u'1999-00', 21)]
El Lugo tuvo las siguientes medias:
[(u'1992-93', 19), (u'2012-13', 21), (u'2013-14', 21), (u'2014-2015', 17)]
El Palamos tuvo las siguientes medias:
[(u'1989-90', 19), (u'1990-91', 19), (u'1991-92', 19), (u'1992-93', 19), (u'1993-94', 19), (u'1994-95', 19)]
El Aragon tuvo las siguientes medias:
[(u'1985-86', 19)]
El Palencia CF tuvo las siguientes medias:
[(u'1979-80', 19), (u'1980-81', 19), (u'1982-83', 19), (u'1983-84', 19)]
El Linares CF tuvo las siguientes medias:
[(u'1973-74', 19), (u'1980-81', 19), (u'1981-82', 19), (u'1982-83', 19), (u'1983-84', 19)]
El Atletico Marbella tuvo las siguientes medias:
[(u'1992-93', 19), (u'1993-94', 19), (u'1994-95', 19), (u'1995-96', 19)]
El Mirandes tuvo las siguientes medias:
[(u'2012-13', 21), (u'2013-14', 21), (u'2014-2015', 17)]
El Ontinyent tuvo las siguientes medias:
[(u'1970-71', 19)]
El AD Almeria tuvo las siguientes medias:
[(u'1978-79', 19), (u'1979-80', 17), (u'1980-81', 17), (u'1981-82', 19)]
El Sevilla Atletico tuvo las siguientes medias:
[(u'2007-08', 21), (u'2008-09', 21)]
El Malaga B tuvo las siguientes medias:
[(u'2003-04', 21), (u'2004-05', 21), (u'2005-06', 21)]
El Vecindario tuvo las siguientes medias:
[(u'2006-07', 21)]
El Real Burgos tuvo las siguientes medias:
[(u'1987-88', 19), (u'1988-89', 19), (u'1989-90', 19), (u'1990-91', 19), (u'1991-92', 19), (u'1992-93', 19), (u'1993-94', 19)]
El Calvo Sotelo tuvo las siguientes medias:
[(u'1970-71', 19), (u'1975-76', 19), (u'1976-77', 19), (u'1977-78', 19), (u'1984-85', 19)]
El Alcorcon tuvo las siguientes medias:
[(u'2010-11', 21), (u'2011-12', 21), (u'2012-13', 21), (u'2013-14', 21), (u'2014-2015', 17)]
El Lorca tuvo las siguientes medias:
[(u'1984-85', 19)]
El Mallorca B tuvo las siguientes medias:
[(u'1998-99', 21)]
'''
