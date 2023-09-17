""" Exercise 1B. Data Processing in IoT.
@author Roberto Hernandez
"""

from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.functions import regexp_replace  # type: ignore

# Local SparkSession
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("ejercicio2")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())

# 1. Input data: DataFrame representing the stream of input lines from socket
df = (spark
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load())

# 2. Data processing: get numbers and add column with the numbers of each line
df2 = df.withColumn('numbers', regexp_replace(df.value, "[^0-9]", ""))

# 3. Output data: show result in the console
query = (df2
         .writeStream
         .outputMode("append")
         .format("console")
         .start())

query.awaitTermination()
