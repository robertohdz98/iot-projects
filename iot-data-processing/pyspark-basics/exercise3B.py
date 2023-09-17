""" Exercise 3B. Data Processing in IoT.
@author Roberto Hernandez
"""

from datetime import timezone

from influxdb_client import InfluxDBClient, Point  # type: ignore
from influxdb_client.client.write_api import SYNCHRONOUS  # type: ignore
from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.functions import regexp_replace  # type: ignore


def saveDataFrameToInfluxDB(dataframe, batchId):
    for row in dataframe.rdd.collect():
        sinValue = float(row["value"])
        timeValue = row["timestamp"].astimezone(timezone.utc)
        print(f"Writing {sinValue} {timeValue} to InfluxDB")
        point = Point("sine-wave").field("value",
                                         sinValue).time(time=timeValue)
        influxClient.write_api(write_options=SYNCHRONOUS).write(
            bucket=bucket, record=point)


# Local SparkSession
spark = (SparkSession
         .builder
         .master("local[*]")
         .appName("Kafka-DataFrame-InfluxDB")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# InfluxDB client (update this info to run this example)
influxClient = InfluxDBClient.from_config_file("../config/influxdb.ini")
bucket = "100449779's Bucket"

# 1. Input data: DataFrame from Apache Kafka
df = (spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test-topic")
      .load())
df.printSchema()

# 2. Data processing: read value and get abs
values = df.withColumn('value', regexp_replace(df.value, "-", " "))

# 3. Output data: store results in InfluxDb
query = (values
         .writeStream
         .outputMode("append")
         .foreachBatch(saveDataFrameToInfluxDB)
         .start())

query.awaitTermination()
