from pyspark.sql import SparkSession
import time
from pyspark.sql.functions import array_contains, split, explode, mean, avg, count
from pyspark.sql.types import ArrayType, StringType

spark = SparkSession.builder.appName("3_ genreaveragefilms").getOrCreate()
sc = spark.sparkContext

t_init = time.time()


# se crear los dataframe de cada tabla 
games = spark.read.format("csv"). \
        options(header="true", inferSchema="true"). \
        load("input/games.csv")
rec = spark.read.format("csv"). \
        options(header="true", inferSchema="true"). \
        load("input/recommendations.csv")
user = spark.read.format("csv"). \
        options(header="true", inferSchema="true"). \
        load("input/users.csv")
meta = spark.read.format("json"). \
        options(header="true", inferSchema="true"). \
        load("input/games_metadata.json")

most = ["Action", "Multiplayer", "Singleplayer", "Adventure", "First-Person", "Co-op", "Open World", "Atmospheric", "FPS", "Shooter"]

#games = games.select("app_id,title","date_release","win","mac","linux","rating","positive_ratio","user_reviews","price_final","price_original","discount","steam_deck")
games = games.select("app_id", "title", "win", "mac", "linux", "rating", "positive_ratio", "user_reviews", "price_original")


#rec = rec.select("app_id","helpful","funny","is_recommended","hours","user_id","review_id")
rec = rec.select("app_id","helpful","funny","is_recommended","hours","user_id")

meta = meta.select("app_id", "description", "tags")

#most = meta.select(explode("tags")).distinct()
#most.foreach(lambda g: meta = meta.withColumn(g, array_contains("tags", g).cast("int")))

#Cambiar for por manejo en rdd
for g in most:
    meta = meta.withColumn(g, array_contains("tags", g).cast("int"))
#meta.withColumn("most_played", most)

meta.drop("tags")

games.show(5)
rec.show(5)
user.show(5)
meta.show(5)

final = rec.join(games,"app_id")
final = final.join(user,"user_id")
final = final.join(meta,"app_id")

