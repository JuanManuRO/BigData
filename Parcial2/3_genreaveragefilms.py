import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.functions import explode, split, avg, count
import re


# Creacion de entornos para el procesamiento de datos
sc = pyspark.SparkContext()
spark = SQLContext(sc)

# Carga de datos manteniendo header = true
rate = spark.read.csv("input/ml-25m/ratings.csv", header = 'true')
movie = spark.read.csv("input/ml-25m/movies.csv", header = 'true')

# Procesamiento inicial de datos; se rompe la columna de generos y se explota como filas bajo el nombre genero
# tambien se guarda en cache para mantener los datos de este dataframe para operaciones futuras con el mismo.
# Se crea dataframe para traducir el genero de las peliculas con el Id para asociar el rating
movie = movie.select("movieId", explode(split(movie.genres, '\|')).alias("genre")).cache()
merged = rate.join(movie, "movieId","left")

# Se generan dos dataframes agrupados bajo genero para extraer el promedio de rating y la cantidad de peliculas
# en cada genero. El promedio se extrae del datafrane combinado y la cantidad directamente de dataframe de peliculas
# para evitar repeticion innecesaria de datos al contar y perdida de algunas peliculas no calificadas
averaged = merged.groupBy("genre").agg(avg("rating").alias("Rating"))
counted = movie.groupBy("genre").agg(count("movieId").alias("Movies"))

# Se juntan los dos dataframes anteriores para el resultado final
final = averaged.join(counted, "genre")

# Escritura de texto linea por linea, no se usan guardados de pyspark al requerir la entrega de un archivo .txt y
# los metodos de pyspark retornan archivos con particiones de texto
f = open("3_out.txt", "x")
final_list = final.collect()
for res in final_list:
    f.write(res[0] + ", " + str(res[1]) + ", " + str(res[2]) + "\n")
f.close()