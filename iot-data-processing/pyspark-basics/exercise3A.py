""" Exercise 3A. Data Processing in IoT.
@author Roberto Hernandez
"""

from pyspark.sql import SparkSession  # type: ignore


def writeToCassandra(dataframe, batchId):
    print(f"Writing DataFrame to Cassandra (micro-batch {batchId})")
    (dataframe
        .write
        .format("org.apache.spark.sql.cassandra")
        .options(keyspace="test", table="numeros")
        .mode("append")
        .save())


# Local SparkSession
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("Rate-DataFrame-Cassandra")
         .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.11:2.4.3")
         .config("spark.cassandra.connection.host", "localhost")
         .config("spark.cassandra.connection.port", "9042")
         .getOrCreate())

# 1. Input data
df = (spark
      .readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load())

# 2. Data processing: add new column with value = df['value'] * 2
final = df.withColumn('double', df["value"] * 2)

# CQL COMMAND TO CREATE TABLE "NUMEROS"
# CREATE TABLE numeros (
#    timestamp timestamp,
#    value int,
#    double int,
#    PRIMARY KEY (timestamp)
# );


# 3. Output data: show results in the console
query = (final
         .writeStream
         .outputMode("update")
         .format("console")
         .foreachBatch(writeToCassandra)
         .start())

query.awaitTermination()
