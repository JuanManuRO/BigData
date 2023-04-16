import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.functions import explode, split, regexp_replace, length, avg, rtrim, ltrim
import re


# Creacion de entornos para el procesamiento de datos
sc = pyspark.SparkContext()
spark = SQLContext(sc)

# Carga de datos con opciones de citas para evitar la lectura de , en reseñas como delimitadores.
# Tambien se usa multiline para indicar que hay datos partidos en varias lineas lo cual ocurre por
# saltos de lineas en reseñas.
games = spark.read.option("quote", "\"").option("escape", "\"").option("multiline", True).csv("input/games.csv", header = 'true')

# Se limpian, parten y explotan los datos de generos y reseñas para poder procesar de
# manera individual cada una de las entradas. Las listas son recibidas como strings lo cual requiere
# borrar los [] y partir por ,. Las reseñas se traducen a longitudes
sep_rev = games.select(
    "Genres", 
    "Rating", 
    explode(split(regexp_replace("Reviews", "[\[\]]", ""), ",")).alias("Review"))
sep_gen = sep_rev.select(
    explode(split(regexp_replace("Genres", "[\[\]]", ""), ",")).alias("Genre"),
    "Rating",
    length("Review"). alias("Length")
)

# Finalmente se agrupan segun generos teniendo en cuenta borrar espacios antes y despues de los strings.
# Tanto la calificacion como las longitudes se promedian
final = sep_gen.groupBy(ltrim(rtrim("Genre"))).agg(avg("Rating").alias("Rating"), avg("Length").alias("Length"))

# Se crea el archivo txt y se extraen los resultados finales en formato de lista para escribirlos en el txt
f = open("5_out.txt", "x")
final_list = final.collect()
for res in final_list:
    f.write(res[0] + ", " + str(res[1]) + ", " + str(res[2]) + "\n")
f.close()