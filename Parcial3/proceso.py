import socket
import time

from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import explode, split, window, count, col, concat_ws

host = '0.tcp.ngrok.io'
port = 12424


if __name__ == "__main__":
    
    spark = SparkSession\
        .builder\
        .appName("ParcialSparkStreaming")\
        .getOrCreate() \
        
    
    spark.sparkContext.setLogLevel("ERROR")
    
    

    windowDuration = '60 seconds'
    slideDuration = '30 seconds'
        
   
    leer = spark.readStream.format("socket").option("host", host)\
             .option("port", port).option("includeTimestamp", 'true').load()

    separar = leer.select(explode(split(leer.value, "[\'\"]\s*b[\'\"]")).alias("birb"), leer.timestamp)
    separar = separar.withWatermark("timestamp", "30 seconds")

    definir = separar.withColumn('id', split(separar['birb'], ',').getItem(0)) \
       .withColumn('time', split(separar['birb'], ',').getItem(1)) \
       .withColumn('simps', split(separar['birb'], ',').getItem(2)) \
        .withColumn('sauce', split(separar['birb'], ',').getItem(3)) \
        .withColumn('rant', split(separar['birb'], ',').getItem(4)) \
        .withColumn('vibes', split(separar['birb'], ',').getItem(5)) \
        .filter(col('id').rlike('^[0-9]+'))

    agrupar = definir\
    .groupBy(window(definir.timestamp, windowDuration, slideDuration), definir.sauce)\
    .agg(count("sauce").alias("count"))\
    .select(concat_ws(",", "window.start", "window.end", "sauce", "count").alias("Result"))
        
    # display all unbounded table completely while streaming the hostCountDF on console using output mode "complete"
    #.union(spark.read.text("/home/jmro/bigdata/parcial3/out.txt").select("value"))\

    agrupar.writeStream\
    .outputMode("append")\
    .foreachBatch(lambda batch_df, batch_id: batch_df\
        .select("result")\
        .coalesce(1)\
        .write\
        .mode("append")\
        .text("/home/hadoop/out.txt"))\
    .start()\
    .awaitTermination(timeout = 60*25)
    

    
