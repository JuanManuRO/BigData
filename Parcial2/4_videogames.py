import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.functions import explode, split, regexp_replace, max_by, rtrim, ltrim
import re

# Creacion de entornos para el procesamiento de datos
sc = pyspark.SparkContext()
spark = SQLContext(sc)

# Carga de datos con opciones de citas para evitar la lectura de , en reseñas como delimitadores.
# Tambien se usa multiline para indicar que hay datos partidos en varias lineas lo cual ocurre por
# saltos de lineas en reseñas.
games = spark.read.option("quote", "\"").option("escape", "\"").option("multiline", True).csv("input/games.csv", header = 'true')

# Se limpian, parten y explotan los datos de generos y equipos para poder procesar de
# manera individual cada una de las entradas. Las listas son recibidas como strings lo cual requiere
# borrar los [] y partir por ,.
team = games.select("Title", explode(split(regexp_replace("Team", "[\[\]]", ""), ",")).alias("Team"), "Plays" )
genre = games.select("Title", explode(split(regexp_replace("Genres", "[\[\]]", ""), ",")).alias("Genre"), "Plays")

# Finalmente se agrupan segun equipos y generos teniendo en cuenta borrar espacios antes y despues de los strings.
# Se agregan los datos usando max_by para encontrar el maximo de veces que se ha jugado el juego reportando el titulo.
max_team = team.groupBy(ltrim(rtrim("Team"))).agg(max_by("Title","Plays").alias("Most Popular"))
max_genre = genre.groupBy(ltrim(rtrim("Genre"))).agg(max_by("Title","Plays").alias("Most Popular"))

# Se crean los archivos txt y se extraen los resultados finales en formato de lista para escribirlos en los txt
f = open("4_genreout.txt", "x")
genre_list = max_genre.collect()
for res in genre_list:
    f.write(res[0] + ", " + str(res[1]) + "\n")
f.close()

f = open("4_teamout.txt", "x")
team_list = max_team.collect()
for res in team_list:
    f.write(res[0] + ", " + str(res[1]) + "\n")
f.close()